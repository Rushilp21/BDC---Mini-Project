export type Candidate = { suffix: string; count: number };
export async function checkPassword(password: string, request: typeof fetch = fetch) {
  const bytes = new TextEncoder().encode(password); // Preserve password case and whitespace.
  const hash = Array.from(new Uint8Array(await crypto.subtle.digest('SHA-1', bytes)))
    .map(b => b.toString(16).padStart(2, '0')).join('').toUpperCase();
  const prefix = hash.slice(0, 5);
  const response = await request(`/api/range/${prefix}`, { method: 'GET', cache: 'no-store' });
  if (!response.ok) throw new Error('Range lookup failed');
  const data = await response.json() as { candidates: Candidate[] };
  const match = data.candidates.find(c => c.suffix === hash.slice(5));
  return { found: !!match, count: match?.count, prefix, candidates: data.candidates.length };
}
