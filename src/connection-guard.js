// Inspect the first packet before minecraft-protocol connects its decoder.
module.exports = function guardConnections(server) {
  const listener = server.socketServer.listeners('connection');
  server.socketServer.removeAllListeners('connection');
  const pending = new Set();
  const close = server.close.bind(server);
  server.close = () => {
    for (const socket of pending) socket.destroy();
    close();
  };
  server.socketServer.on('connection', (socket) => {
    pending.add(socket);
    const prefix = [];
    let length = 0;
    let lengthBytes = 0;
    const timer = setTimeout(() => socket.destroy(), 10000);
    const cleanup = () => {
      clearTimeout(timer);
      pending.delete(socket);
      socket.removeListener('readable', inspect);
      socket.removeListener('error', reject);
      socket.removeListener('close', cleanup);
    };
    const reject = () => socket.destroy();
    function inspect() {
      let byte;
      while ((byte = socket.read(1)) !== null) {
        const value = byte[0];
        prefix.push(value);
        if (!lengthBytes) {
          // The handshake has a bounded hostname, not an arbitrary payload.
          length += (value & 0x7f) * 2 ** (7 * (prefix.length - 1));
          if (prefix.length > 3 || length > 1030) return reject();
          if (value & 0x80) continue;
          if (length < 3) return reject();
          lengthBytes = prefix.length;
          continue;
        }
        // HTTP (GET, POST, etc.) and TLS are not Minecraft handshakes.
        if (value !== 0) return reject();
        cleanup();
        socket.unshift(Buffer.from(prefix));
        for (const accept of listener) accept.call(server.socketServer, socket);
        return;
      }
    }
    socket.on('error', reject);
    socket.once('close', cleanup);
    socket.on('readable', inspect);
  });
};
