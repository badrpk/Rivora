const test = require("node:test");
const assert = require("node:assert");
const { server } = require("./server");
test("server exports", () => {
  assert.ok(server);
});
