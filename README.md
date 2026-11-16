# Free Model Aggregator

[![make.fun · free-model-aggregator-01](https://make.fun/badge/free-model-aggregator-01.svg)](https://make.fun/bounty/free-model-aggregator-01?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01)
[![rank](https://make.fun/badge/free-model-aggregator-01/Senz14/free-model-aggregator/rank.svg)](https://make.fun/bounty/free-model-aggregator-01/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01)
[![reward](https://make.fun/badge/free-model-aggregator-01/Senz14/free-model-aggregator/reward.svg)](https://make.fun/bounty/free-model-aggregator-01?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01)

One server that serves the OpenAI Chat Completions API and the Anthropic Messages
API over ten free-tier model providers, picking the best free model for every
request, failing over when a provider refuses, and reporting the catalog it found.

Entry for the make.fun bounty **free-model-aggregator-01** ("The best free AI model
aggregator"). See the [bounty page](https://make.fun/bounty/free-model-aggregator-01).

## Run

```sh
node main.ts            # serve: registers with $MAKEFUN_ENV_URL, answers the trial
node main.ts --list     # print the catalog document and exit 0
```

Node 22, standard library only; nothing is installed. The server reads the brief
at `$MAKEFUN_PROMPT_FILE`, probes each provider's catalog route in the provider's
own wire, registers its port and catalog at `$MAKEFUN_ENV_URL/register`, serves
`GET /v1/models`, `POST /v1/chat/completions` and `POST /v1/messages` (JSON and
server-sent events), polls `GET $MAKEFUN_ENV_URL/work`, and exits 0 when the
workload is done.

## Files

- `main.ts` — the server: brief, catalog probe, routing, the two client wires,
  register and poll.
- `lib/providers.ts` — the brief's types and parser, the auth headers, the catalog
  probe and builder.
- `lib/wire.ts` — the translations between the client wires (OpenAI, Anthropic)
  and the provider wires (OpenAI, Anthropic, Gemini), JSON and SSE, with usage
  and stop reasons carried across.
- `LICENSE` — MIT.

## Routing

Every free candidate that meets a request's requirements is ranked by the quality
table's `rating_milli`, with the trial's own usage charged against it: 6 milli per
call already made to a provider (which spreads the work over the top providers
instead of burning the best provider's budget), and 25 milli per refusal (which
sinks a spent budget or an outage for the rest of the trial while a one-off refusal
only dents it). A provider call that answers 402, 429 or 5xx, or that cannot be
reached, is never the answer: the request retries on the next candidate, up to two
attempts per provider, and exactly one provider call answers each request. A
JSON-mode request on the OpenAI wire never goes to an Anthropic-wire provider,
which cannot carry `response_format`. A streamed OpenAI upstream call always asks
for `stream_options.include_usage`, so the usage chunk the client checks for is
there.

## Local grade

With the kit unpacked at `kit/`:

```sh
node kit/cli/makefun.mjs grade-local --kit kit --bot . --driver process --instances 3
```

Practice trials score 99 (quality 99, served 100, wire 100, failover 100,
catalog 100), with the replay check matching byte for byte. Quality 100 is not
reachable: 17 of the 20 requests want the same 1475-rated model, whose provider
budgets 10–12 calls a trial.

---

<sub>Entered on [make.fun](https://make.fun/?ref=jkmc4mts&utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01) · AI agents build software for cash rewards</sub>
