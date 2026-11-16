// free-model-aggregator-01: one server, two client wires, ten free providers.
//
// The trial gives this server a brief and an env, and runs it as `node /bot/main.ts` from the
// repository's root, with Node 22's standard library alone (nothing is installed; TypeScript runs
// directly, and a relative import needs its .ts extension). The brief is at the path in
// $MAKEFUN_PROMPT_FILE. It carries three JSON documents: the provider directory (ten simulated
// free-tier providers, each with its id, wire, auth scheme, key environment variable, the key the
// service takes, and the base path joined to $MAKEFUN_ENV_URL), the quality table (every model's
// public rating, what it can do, and whether it is free), and the workload manifest (the twenty
// requests the client will send, in order, with each one's api, whether it streams, its weight and
// the requirements a model must meet to serve it).
//
// The server reads the brief, probes each provider's catalog route in the provider's own wire,
// builds one catalog, then:
//   1. listens on 0.0.0.0 at a free port (never 8080: the env itself is there on one layout);
//   2. POSTs $MAKEFUN_ENV_URL/register {"protocol_version": 1, "port": <port>, "catalog": <catalog>};
//   3. serves the client's requests: GET /v1/models (answering in the shape of the auth header it
//      got: x-api-key means Anthropic, Authorization means OpenAI), POST /v1/chat/completions and
//      POST /v1/messages, each with a JSON body or with server-sent events;
//   4. polls GET $MAKEFUN_ENV_URL/work every 100 ms and exits 0 when the env says {"done": true}. A
//      process still running when the wall ends scores zero, so never wait for anything else.
// `node main.ts --list` prints the same catalog JSON and exits 0 before any server starts; the
// register body's catalog must be that same document.
//
// ROUTING (what the score pays for)
//   - quality (60%): every free candidate that meets the request's requirements is ranked by the
//     quality table's rating_milli. The best model wins, but each provider's free tier is a budget
//     the trial never publishes, so a provider that has already answered calls is charged a small
//     penalty per call (6 milli), which spreads the work over the top providers at a quality cost
//     of a few milli per request instead of burning the best provider's budget and failing over
//     late. A provider that refused a call is charged a larger penalty (25 milli per refusal), so
//     a spent budget or an outage sinks it for the rest of the trial while a one-off refusal
//     (a failure on the provider's schedule) only dents it.
//   - failover (10%) and served (15%): a provider call that answers 402, 429 or 5xx, or that
//     cannot be reached, is never the answer. The request is retried on the next candidate, up to
//     two attempts per provider (a refusal on the provider's failure schedule is one call number,
//     so its next model may still answer). Exactly one provider call may answer a request: the
//     loop stops at the first success.
//   - wire (10%): every provider wire is spoken (openai, anthropic and gemini), and every answer
//     is translated back into the wire the client used, JSON or SSE, with the provider's text
//     whole, its usage counts as that wire reports them, and its stop reason mapped. A streamed
//     OpenAI upstream call always asks for stream_options.include_usage, so the usage chunk the
//     client checks for is there. A JSON-mode request on the OpenAI wire never goes to an
//     Anthropic-wire provider: that wire cannot carry response_format, and the provider would
//     echo json_mode false.
//   - catalog (5%): the probe reports what each listing said, with the quality table's free flag
//     and capabilities where it has a row.
//
// Everything is deterministic: no wall clock and no unseeded randomness, so a replay leaves the
// same state export. The one order is kept by serving one client request at a time (the client
// sends them that way) and by making provider calls one at a time, in the candidate order.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import {
  authHeaders,
  buildCatalog,
  capabilitiesOf,
  isRecord,
  joinBase,
  meetsRequirements,
  modelFree,
  probeProvider,
  ratingOfModel,
  readBrief,
  type CatalogReport,
  type Json,
  type Probe,
  type ProbedModel,
  type Requirement,
} from './lib/providers.ts';
import {
  anthropicError,
  anthropicEventsFromGemini,
  anthropicEventsFromOpenAI,
  anthropicModelList,
  anthropicToGemini,
  anthropicToOpenAI,
  anthropicToOpenAIResponse,
  geminiToAnthropic,
  geminiToOpenAI,
  openAIError,
  openAIEventsFromAnthropic,
  openAIEventsFromGemini,
  openAIModelList,
  openAIToAnthropic,
  openAIToAnthropicRequest,
  openAIToGemini,
  type ServedModel,
} from './lib/wire.ts';

/** The wire protocol's version, in the register body. */
const PROTOCOL_VERSION = 1;
/** How often GET /work is polled, in milliseconds. */
const POLL_INTERVAL_MS = 100;
/** How long one provider's catalog probe may take, in milliseconds. */
const PROBE_TIMEOUT_MS = 5000;
/** How long one provider inference call may take, in milliseconds. */
const CALL_TIMEOUT_MS = 8000;
/** The most bytes of one client request body this server reads. */
const MAX_BODY_BYTES = 32 * 1024 * 1024;
/** Rating milli charged per provider call already made: spreads the work over the top providers. */
const PENALTY_PER_CALL_MILLI = 6;
/** Rating milli charged per provider refusal: a spent budget or an outage sinks a provider. */
const PENALTY_PER_REFUSAL_MILLI = 25;
/** The most attempts one provider gets for one request. */
const MAX_ATTEMPTS_PER_PROVIDER = 2;

const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');
const PROMPT_FILE = process.env.MAKEFUN_PROMPT_FILE ?? '';

const brief = readBrief(PROMPT_FILE);
let probes: Probe[] = [];
let catalog: CatalogReport = { providers: [] };
/** How many client API requests have arrived: the manifest is in arrival order, one request at a time. */
let manifestIndex = 0;
/** Inference calls made to each provider so far in this trial (successes and refusals alike). */
const callsMade = new Map<string, number>();
/** Provider refusals (402, 429, 5xx, unreachable) so far in this trial. */
const refusals = new Map<string, number>();

function log(message: string): void {
  process.stderr.write(`aggregator: ${message}\n`);
}

/** Probe every provider and build the one catalog (the `--list` document and the register body). */
async function load(): Promise<void> {
  if (brief.error !== null) log(brief.error);
  probes = await Promise.all(brief.directory.providers.map((provider) => probeProvider(ENV_URL, provider, PROBE_TIMEOUT_MS)));
  for (const probe of probes) if (!probe.ok && probe.error !== null) log(probe.error);
  catalog = buildCatalog(ENV_URL, probes, brief.ratings);
}

const byId = (a: ProbedModel, b: ProbedModel): number => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);

/** The free models across the providers that answered, in the brief's provider order: what GET /v1/models lists. */
function servedModels(): ServedModel[] {
  const out: ServedModel[] = [];
  for (const probe of probes) {
    if (!probe.ok) continue;
    for (const model of [...probe.models].sort(byId)) {
      const row = ratingOfModel(brief.ratings, probe.provider.id, model.id);
      if (!modelFree(model, row)) continue;
      out.push({ id: model.id, provider: probe.provider.id, display_name: model.display_name });
    }
  }
  return out;
}

interface Candidate {
  probe: Probe;
  model: ProbedModel;
  rating: number;
}

/** Whether the client's OpenAI body asks for JSON mode: only the openai and gemini wires can carry it. */
function asksJsonMode(clientApi: 'openai' | 'anthropic', body: Json): boolean {
  if (clientApi !== 'openai') return false;
  const format = isRecord(body.response_format) ? body.response_format.type : undefined;
  return format === 'json_object' || format === 'json_schema';
}

/**
 * The free candidates for one request, best first. The order is the quality table's rating with
 * the trial's own usage charged against it: a provider that has already answered calls, or that
 * has refused, is worth less than its rating says. Ties break toward the provider with fewer
 * calls, then by provider and model id, so the order is a function of the brief and the answers
 * seen so far, never of the clock.
 */
function candidatesFor(requires: Requirement, clientApi: 'openai' | 'anthropic', body: Json): Candidate[] {
  const jsonMode = asksJsonMode(clientApi, body);
  const out: Candidate[] = [];
  for (const probe of probes) {
    if (!probe.ok) continue;
    if (jsonMode && probe.provider.wire === 'anthropic') continue;
    for (const model of probe.models) {
      const row = ratingOfModel(brief.ratings, probe.provider.id, model.id);
      if (!modelFree(model, row)) continue;
      if (!meetsRequirements(requires, capabilitiesOf(model, row))) continue;
      out.push({ probe, model, rating: row?.rating_milli ?? 0 });
    }
  }
  const effective = (c: Candidate): number =>
    c.rating - PENALTY_PER_CALL_MILLI * (callsMade.get(c.probe.provider.id) ?? 0) - PENALTY_PER_REFUSAL_MILLI * (refusals.get(c.probe.provider.id) ?? 0);
  out.sort((a, b) => {
    const byRating = effective(b) - effective(a);
    if (byRating !== 0) return byRating;
    const byCalls = (callsMade.get(a.probe.provider.id) ?? 0) - (callsMade.get(b.probe.provider.id) ?? 0);
    if (byCalls !== 0) return byCalls;
    if (a.probe.provider.id !== b.probe.provider.id) return a.probe.provider.id < b.probe.provider.id ? -1 : 1;
    return a.model.id < b.model.id ? -1 : a.model.id > b.model.id ? 1 : 0;
  });
  return out;
}

/** The requirements of the request now arriving: the manifest's row by arrival order, else the body's own. */
function requirementsFor(body: Json, clientApi: 'openai' | 'anthropic'): Requirement {
  const manifest = brief.workload?.manifest ?? [];
  const entry = manifest[manifestIndex];
  manifestIndex += 1;
  if (entry !== undefined && entry.api === clientApi) return entry.requires;
  // The manifest is the declared source of each request's requirements; this fallback is a
  // guess for a brief that holds none (it cannot see a min_context).
  return requirementsFromBody(body);
}

/** Requirements read off a request body, when the manifest has no row for it. */
function requirementsFromBody(body: Json): Requirement {
  const messages = Array.isArray(body.messages) ? body.messages : [];
  let vision = false;
  for (const message of messages) {
    if (!isRecord(message) || !Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (isRecord(block) && (block.type === 'image' || block.type === 'image_url')) vision = true;
    }
  }
  const format = isRecord(body.response_format) ? body.response_format.type : undefined;
  return { tools: Array.isArray(body.tools) && body.tools.length > 0, vision, json: format === 'json_object' || format === 'json_schema' };
}

/** One client request body, or null when it is not one JSON object or is too large. */
async function readJson(req: IncomingMessage): Promise<Json | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    size += buffer.length;
    if (size > MAX_BODY_BYTES) return null;
    chunks.push(buffer);
  }
  if (chunks.length === 0) return null;
  try {
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return isRecord(value) ? value : null;
  } catch {
    return null;
  }
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text) });
  res.end(text);
}

function sendError(res: ServerResponse, wire: 'openai' | 'anthropic', status: number, type: string, message: string): void {
  if (wire === 'anthropic') sendJson(res, status, anthropicError(type, message));
  else sendJson(res, status, openAIError(message, type, null));
}

/** GET /v1/models, in the shape of the auth header the client sent. */
function models(req: IncomingMessage, res: ServerResponse): void {
  const list = servedModels();
  if (typeof req.headers['x-api-key'] === 'string') sendJson(res, 200, anthropicModelList(list));
  else sendJson(res, 200, openAIModelList(list));
}

/** The upstream request for one candidate: the client's body in the provider's own wire. */
function upstreamRequest(candidate: Candidate, clientApi: 'openai' | 'anthropic', body: Json): { url: string; headers: Record<string, string>; payload: Json } {
  const provider = candidate.probe.provider;
  const base = joinBase(ENV_URL, provider.base);
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    ...authHeaders(provider.auth, provider.key),
  };
  if (provider.wire === 'anthropic') headers['anthropic-version'] = '2023-06-01';
  if (provider.wire === 'openai') {
    const payload = clientApi === 'openai' ? { ...body, model: candidate.model.id } : anthropicToOpenAI(body, candidate.model.id);
    if (body.stream === true) {
      // The client checks the stream's usage: ask the provider to send its usage chunk.
      payload.stream_options = { include_usage: true };
    }
    return { url: `${base}/v1/chat/completions`, headers, payload };
  }
  if (provider.wire === 'anthropic') {
    const payload = clientApi === 'openai' ? openAIToAnthropicRequest(body, candidate.model.id) : { ...body, model: candidate.model.id };
    return { url: `${base}/v1/messages`, headers, payload };
  }
  const payload = clientApi === 'openai' ? openAIToGemini(body) : anthropicToGemini(body);
  const action = body.stream === true ? 'streamGenerateContent' : 'generateContent';
  const path = `/v1beta/models/${encodeURIComponent(candidate.model.id)}:${action}`;
  const url = body.stream === true ? `${base}${path}?alt=sse` : `${base}${path}`;
  return { url, headers, payload };
}

/** One provider call: the response, or null when the call itself failed. */
async function callProvider(candidate: Candidate, clientApi: 'openai' | 'anthropic', body: Json): Promise<Response | null> {
  const { url, headers, payload } = upstreamRequest(candidate, clientApi, body);
  callsMade.set(candidate.probe.provider.id, (callsMade.get(candidate.probe.provider.id) ?? 0) + 1);
  try {
    return await fetch(url, { method: 'POST', headers, body: JSON.stringify(payload), signal: AbortSignal.timeout(CALL_TIMEOUT_MS) });
  } catch {
    return null;
  }
}

/** Copy a provider's OpenAI event stream to the client as it arrives. */
async function pipeStream(stream: ReadableStream<Uint8Array>, res: ServerResponse): Promise<void> {
  const reader = stream.getReader();
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      res.write(Buffer.from(value));
    }
  } finally {
    reader.releaseLock();
    res.end();
  }
}

/** Answer the client from one provider's JSON answer, in the client's wire. */
function answerJson(res: ServerResponse, clientApi: 'openai' | 'anthropic', candidate: Candidate, answer: Json): void {
  const wire = candidate.probe.provider.wire;
  if (clientApi === 'openai') {
    if (wire === 'openai') sendJson(res, 200, answer);
    else if (wire === 'anthropic') sendJson(res, 200, anthropicToOpenAIResponse(answer, candidate.model.id));
    else sendJson(res, 200, geminiToOpenAI(answer, candidate.model.id));
    return;
  }
  if (wire === 'openai') sendJson(res, 200, openAIToAnthropic(answer, candidate.model.id));
  else if (wire === 'anthropic') sendJson(res, 200, answer);
  else sendJson(res, 200, geminiToAnthropic(answer, candidate.model.id));
}

/** Answer the client from one provider's SSE stream, in the client's wire. */
async function answerStream(res: ServerResponse, clientApi: 'openai' | 'anthropic', candidate: Candidate, upstream: Response): Promise<void> {
  const wire = candidate.probe.provider.wire;
  res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
  if (upstream.body === null) {
    res.end();
    return;
  }
  // Same wire both ends: the provider's events are the client's events.
  if (clientApi === 'openai' && wire === 'openai') {
    await pipeStream(upstream.body, res);
    return;
  }
  if (clientApi === 'anthropic' && wire === 'anthropic') {
    await pipeStream(upstream.body, res);
    return;
  }
  const events =
    clientApi === 'openai'
      ? wire === 'anthropic'
        ? openAIEventsFromAnthropic(upstream.body, candidate.model.id)
        : openAIEventsFromGemini(upstream.body, candidate.model.id)
      : wire === 'openai'
        ? anthropicEventsFromOpenAI(upstream.body, candidate.model.id)
        : anthropicEventsFromGemini(upstream.body, candidate.model.id);
  for await (const event of events) res.write(event);
  res.end();
}

/**
 * Serve one client request: try the candidates in order until one answers, failing over on a
 * refusal. Exactly one provider call answers the request; a request no candidate answers gets a
 * 503 in the client's wire.
 */
async function serve(req: IncomingMessage, res: ServerResponse, clientApi: 'openai' | 'anthropic', body: Json): Promise<void> {
  const stream = body.stream === true;
  const candidates = candidatesFor(requirementsFor(body, clientApi), clientApi, body);
  const attempts = new Map<string, number>();
  for (const candidate of candidates) {
    const providerId = candidate.probe.provider.id;
    if ((attempts.get(providerId) ?? 0) >= MAX_ATTEMPTS_PER_PROVIDER) continue;
    attempts.set(providerId, (attempts.get(providerId) ?? 0) + 1);
    let upstream: Response | null;
    try {
      upstream = await callProvider(candidate, clientApi, body);
    } catch {
      upstream = null;
    }
    if (upstream === null) {
      refusals.set(providerId, (refusals.get(providerId) ?? 0) + 1);
      log(`${providerId}/${candidate.model.id} unreachable, failing over`);
      continue;
    }
    if (!upstream.ok) {
      refusals.set(providerId, (refusals.get(providerId) ?? 0) + 1);
      log(`${providerId}/${candidate.model.id} refused HTTP ${upstream.status}, failing over`);
      try {
        await upstream.arrayBuffer();
      } catch {
        // The error body is only drained to free the connection.
      }
      continue;
    }
    if (stream) {
      await answerStream(res, clientApi, candidate, upstream);
      return;
    }
    let answer: unknown;
    try {
      answer = await upstream.json();
    } catch {
      refusals.set(providerId, (refusals.get(providerId) ?? 0) + 1);
      log(`${providerId}/${candidate.model.id} answered unparseable JSON, failing over`);
      continue;
    }
    if (!isRecord(answer)) {
      refusals.set(providerId, (refusals.get(providerId) ?? 0) + 1);
      log(`${providerId}/${candidate.model.id} answered a JSON value that is not one object, failing over`);
      continue;
    }
    answerJson(res, clientApi, candidate, answer);
    return;
  }
  log(`no candidate answered the ${clientApi} request`);
  sendError(res, clientApi, 503, clientApi === 'anthropic' ? 'overloaded_error' : 'no_free_model', 'no free model on the directory answered this request');
}

/** POST /v1/chat/completions and POST /v1/messages: read the body, then serve it. */
async function handlePost(req: IncomingMessage, res: ServerResponse, clientApi: 'openai' | 'anthropic'): Promise<void> {
  const body = await readJson(req);
  if (body === null) return sendError(res, clientApi, 400, 'invalid_request_error', 'the request body is not one JSON object');
  await serve(req, res, clientApi, body);
}

async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://entry');
  if (req.method === 'GET' && url.pathname === '/v1/models') {
    models(req, res);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/chat/completions') {
    await handlePost(req, res, 'openai');
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/messages') {
    await handlePost(req, res, 'anthropic');
    return;
  }
  sendError(res, url.pathname === '/v1/messages' ? 'anthropic' : 'openai', 404, 'not_found', `no route ${req.method ?? ''} ${url.pathname}`);
}

function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '0.0.0.0', () => resolve());
  });
}

/** POST /register with the port and the one catalog; the env answers with the workload's size. */
async function register(port: number, report: CatalogReport): Promise<void> {
  const res = await fetch(`${ENV_URL}/register`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ protocol_version: PROTOCOL_VERSION, port, catalog: report }),
    signal: AbortSignal.timeout(5000),
  });
  if (!res.ok) throw new Error(`POST ${ENV_URL}/register answered HTTP ${res.status}: ${(await res.text()).slice(0, 500)}`);
  const reply: unknown = await res.json();
  if (isRecord(reply) && typeof reply.requests === 'number') log(`registered: the workload holds ${reply.requests} requests`);
}

/** Poll GET /work every 100 ms; exit 0 the moment it says done. */
function poll(server: Server): void {
  let inFlight = false;
  const timer = setInterval(() => {
    if (inFlight) return;
    inFlight = true;
    void (async () => {
      try {
        const res = await fetch(`${ENV_URL}/work`, { signal: AbortSignal.timeout(2000) });
        if (res.status !== 200) return;
        const work: unknown = await res.json();
        if (isRecord(work) && work.done === true) {
          clearInterval(timer);
          server.close();
          process.exit(0);
        }
      } catch {
        // The env is not reachable yet: try again on the next tick.
      } finally {
        inFlight = false;
      }
    })();
  }, POLL_INTERVAL_MS);
}

async function start(): Promise<void> {
  await load();
  if (process.argv.includes('--list')) {
    // The same document the register body carries, byte for byte: print the one built object and stop
    // before any server starts. A process whose stdout is not flushed loses the tail, so wait for it.
    const text = `${JSON.stringify(catalog)}\n`;
    await new Promise<void>((resolve) => process.stdout.write(text, () => resolve()));
    process.exit(0);
  }
  if (ENV_URL === '') throw new Error('MAKEFUN_ENV_URL is not set');
  const server = createServer((req, res) => {
    void handle(req, res).catch((err: unknown) => {
      log((err as Error).stack ?? String(err));
      if (!res.headersSent) sendError(res, req.url?.startsWith('/v1/messages') === true ? 'anthropic' : 'openai', 500, 'internal_error', 'the server failed to answer');
    });
  });
  await listen(server);
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('the server has no port');
  const port = address.port;
  // 8080 is the env's on one layout; the OS's ephemeral range never holds it, and the guard says so.
  if (port === 8080) throw new Error('the OS picked 8080, which the env owns on one layout: restart the trial');
  log(`listening on 0.0.0.0:${port} with ${catalog.providers.length} answered providers and ${servedModels().length} free models`);
  await register(port, catalog);
  process.on('SIGTERM', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0));
  poll(server);
}

try {
  await start();
} catch (err) {
  log((err as Error).stack ?? String(err));
  process.exit(1);
}
