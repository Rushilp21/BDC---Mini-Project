import { describe, it, expect, vi } from 'vitest';
import { webcrypto } from 'node:crypto';
import { checkPassword } from './privacy';
Object.defineProperty(globalThis, 'crypto', { value: webcrypto });
describe('private checker', () => {
  it('sends only five characters and matches suffix locally', async () => {
    const request = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ candidates: [{ suffix: '1E4C9B93F3F0682250B6CF8331B7EE68FD8', count: 42 }] }) });
    const result = await checkPassword('password', request);
    expect(result.found).toBe(true);
    expect(request.mock.calls).toEqual([['/api/range/5BAA6', { method: 'GET', cache: 'no-store' }]]);
    expect(JSON.stringify(request.mock.calls)).not.toContain('password');
  });
  it('preserves whitespace and case', async () => {
    const request = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ candidates: [] }) });
    expect((await checkPassword(' Password ', request)).found).toBe(false);
    expect(request.mock.calls[0][0]).not.toBe('/api/range/5BAA6');
  });
});
