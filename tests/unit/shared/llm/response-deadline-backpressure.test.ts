import { createServer, type ServerResponse } from 'node:http';
import { once } from 'node:events';
import type { Socket } from 'node:net';
import { describe, expect, it } from 'vitest';
import { fetchWithResponseDeadline } from '../../../../src/shared/llm/providers/response-deadline';
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function endpoint(reply: (res: ServerResponse) => void, run: (url: string) => Promise<void>) {
  const sockets = new Set<Socket>();
  const server = createServer((_req, res) => reply(res));
  server.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('missing fixture port');
  try { await run(`http://127.0.0.1:${address.port}`); }
  finally { for (const s of sockets) s.destroy(); await new Promise<void>(resolve => server.close(() => resolve())); }
}
const timeout = () => new Error('owned idle timeout');
describe('#962 idle deadlines exclude consumer backpressure', () => {
  it('does not time out while the consumer pauses after a received chunk', async () => {
    await endpoint(res => res.end('data: first\n\ndata: second\n\n'), async url => {
      const response = await fetchWithResponseDeadline(url, {}, 100, timeout, 'idle');
      const reader = response.body!.getReader();
      expect((await reader.read()).done).toBe(false);
      await delay(200);
      expect((await reader.read()).done).toBe(true);
    });
  });
  it('does not time out before the consumer requests the first body chunk', async () => {
    await endpoint(res => res.end('complete body'), async url => {
      const response = await fetchWithResponseDeadline(url, {}, 100, timeout, 'idle');
      await delay(200);
      expect(await response.text()).toBe('complete body');
    });
  });
  it('still times out a pending read when the network stops sending bytes', async () => {
    await endpoint(res => { res.writeHead(200); res.write('first'); }, async url => {
      const response = await fetchWithResponseDeadline(url, {}, 100, timeout, 'idle');
      const reader = response.body!.getReader(); await reader.read();
      await expect(reader.read()).rejects.toThrow('owned idle timeout');
    });
  });
  it('preserves a network read failure instead of reclassifying it as idle timeout', async () => {
    await endpoint(res => { res.writeHead(200); res.write('first'); setTimeout(() => res.destroy(), 30); }, async url => {
      const response = await fetchWithResponseDeadline(url, {}, 500, timeout, 'idle');
      const reader = response.body!.getReader(); await reader.read();
      await expect(reader.read()).rejects.not.toThrow('owned idle timeout');
    });
  });
  it('consumer cancellation closes the stream without a later idle error', async () => {
    await endpoint(res => { res.writeHead(200); res.write('first'); }, async url => {
      const response = await fetchWithResponseDeadline(url, {}, 100, timeout, 'idle');
      const reader = response.body!.getReader(); await reader.read();
      await reader.cancel('owned consumer stop');
      await delay(200);
      expect((await reader.read()).done).toBe(true);
    });
  });
  it('preserves an invalid-request failure rather than calling it a timeout', async () => {
    await expect(fetchWithResponseDeadline('not a valid URL', {}, 100, timeout, 'idle'))
      .rejects.not.toThrow('owned idle timeout');
  });
  it.each(['absolute', 'idle'] as const)('preserves the absolute request cap while paused (%s)', async mode => {
    await endpoint(res => res.end('complete body'), async url => {
      const response = await fetchWithResponseDeadline(url, {}, 100, timeout, mode, 100);
      const reader = response.body!.getReader(); await reader.read();
      await delay(200);
      await expect(reader.read()).rejects.toThrow('owned idle timeout');
    });
  });
});
