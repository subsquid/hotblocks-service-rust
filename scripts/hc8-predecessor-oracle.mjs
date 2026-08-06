// Temporary REQ-24 oracle. Remove this script and its nightly workflow once
// predecessor wire compatibility is sunset.

import {readFileSync, writeFileSync} from 'node:fs'
import {createRequire} from 'node:module'
import {join, resolve} from 'node:path'

const [packageDirArg, casesPath, outputDir] = process.argv.slice(2)
if (!packageDirArg || !casesPath || !outputDir) {
    throw new Error('usage: node hc8-predecessor-oracle.mjs <evm-data-service-dir> <cases.json> <output-dir>')
}

const packageDir = resolve(packageDirArg)
const requireFromPredecessor = createRequire(join(packageDir, 'package.json'))
const {mapRpcBlock} = requireFromPredecessor('@subsquid/evm-normalization')
const {toJSON} = requireFromPredecessor('@subsquid/util-internal-json')
const {Metrics} = requireFromPredecessor('@subsquid/util-internal-data-service')
const {createHttpApp} = requireFromPredecessor(
    '@subsquid/util-internal-data-service/lib/http-app.js',
)

const cases = JSON.parse(readFileSync(casesPath, 'utf8'))
const payloads = []
const HTTP_TIMEOUT_MS = 10_000

async function withTimeout(promise, operation) {
    let timeout
    try {
        return await Promise.race([
            promise,
            new Promise((_, reject) => {
                timeout = setTimeout(
                    () => reject(new Error(`${operation} timed out`)),
                    HTTP_TIMEOUT_MS,
                )
            }),
        ])
    } finally {
        clearTimeout(timeout)
    }
}

for (let caseIndex = 0; caseIndex < cases.length; caseIndex++) {
    const testCase = cases[caseIndex]
    const rawBlocks = JSON.parse(readFileSync(testCase.rawBlocksFile, 'utf8'))
    if (!Array.isArray(rawBlocks)) {
        throw new Error(`missing raw blocks for case ${caseIndex}`)
    }
    for (let fixtureIndex = 0; fixtureIndex < rawBlocks.length; fixtureIndex++) {
        const normalized = mapRpcBlock(rawBlocks[fixtureIndex], {
            withTraces: testCase.withTraces,
            withStateDiffs: testCase.withStateDiffs,
        })
        const file = `payload-${caseIndex}-${fixtureIndex}.jsonl`
        writeFileSync(join(outputDir, file), JSON.stringify(toJSON(normalized)) + '\n')
        payloads.push({
            caseIndex,
            fixtureIndex,
            file,
        })
    }
}

const metrics = new Metrics()
const service = {
    metrics,
    async query() {
        throw new Error('query must not run in the oracle probes')
    },
    getHead() {
        throw new Error('head must not run in the oracle probes')
    },
    getFinalizedHead() {
        throw new Error('finalized head must not run in the oracle probes')
    },
    async isReady() {
        throw new Error('readiness must not run in the oracle probes')
    },
}

const server = await withTimeout(createHttpApp(service).listen(0), 'oracle server startup')
const baseUrl = `http://127.0.0.1:${server.port}`

try {
    const metricsResponse = await fetch(`${baseUrl}/metrics?json=true`, {
        signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
    })
    const metricFamilies = (await metricsResponse.json()).filter((family) =>
        family.name.startsWith('sqd_hotblocks_'),
    )
    const oversizedResponse = await fetch(`${baseUrl}/stream`, {
        method: 'POST',
        headers: {'content-type': 'application/json'},
        body: 'x'.repeat(1025),
        signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
    })

    writeFileSync(
        join(outputDir, 'manifest.json'),
        JSON.stringify({
            payloads,
            http: {
                metrics: {
                    status: metricsResponse.status,
                    contentType: metricsResponse.headers.get('content-type'),
                    families: metricFamilies,
                },
                oversized: {
                    status: oversizedResponse.status,
                    contentType: oversizedResponse.headers.get('content-type'),
                },
            },
        }),
    )
} finally {
    await withTimeout(server.close(), 'oracle server shutdown')
}

process.exit(0)
