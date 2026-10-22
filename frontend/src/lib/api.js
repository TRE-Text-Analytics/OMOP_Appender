/**
 * Core API utilities shared across all features.
 * Add any new fetch helpers here rather than inline in components.
 */

const BASE = '/api'

/** POST JSON to a URL and return the raw Response (for streaming or .json()). */
export async function postJSON(path, body) {
  return fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
}

/** GET from a URL and return parsed JSON. */
export async function getJSON(path) {
  const res = await fetch(`${BASE}${path}`)
  return res.json()
}

/**
 * Read a streaming Response as newline-delimited JSON.
 * Calls onEvent(parsedObject) for each complete JSON line received.
 */
export async function readNDJSON(response, onEvent) {
  const reader  = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer    = ''

  while (true) {
    const { done, value } = await reader.read()
    if (done) break

    buffer += decoder.decode(value, { stream: true })
    const lines = buffer.split('\n')
    buffer = lines.pop() // keep incomplete trailing line

    for (const line of lines) {
      if (!line.trim()) continue
      try { onEvent(JSON.parse(line)) } catch { /* skip malformed */ }
    }
  }
}
