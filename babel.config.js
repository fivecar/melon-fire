module.exports = {
  env: {
    test: {
      presets: ["module:metro-react-native-babel-preset"],
    },
  },
  presets: [
    ["@babel/preset-env", { targets: { node: "current" }, modules: false }],
  ],
};
