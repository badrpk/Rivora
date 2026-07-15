const http = require("http");
const port = process.env.PORT || 8765;
const server = http.createServer((req, res) => {
  const body = JSON.stringify({
    ok: true,
    service: "rangoons-live",
    title: "Rangoons Live",
    description: "Live commerce storefront (flash sales + livestream checkout) paired with but separate from rangoons-core backend.",
    health: "pass",
    path: req.url,
  }, null, 2);
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(body);
});
if (require.main === module) {
  server.listen(port, "127.0.0.1", () => console.log("Rangoons Live on http://127.0.0.1:" + port));
}
module.exports = { server };
