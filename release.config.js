const config = {
  branches: ["main"],
  plugins: [
    ["@semantic-release/commit-analyzer", { preset: "conventionalcommits" }],
    [
      "@semantic-release/release-notes-generator",
      { preset: "conventionalcommits" },
    ],
    [
      "@semantic-release/github",
      {
        assets: [
          { path: "redis/redis-server-go", label: "redis-server-go" },
          { path: "wc/ccwc", label: "ccwc" },
        ],
      },
    ],
  ],
};

module.exports = config;
