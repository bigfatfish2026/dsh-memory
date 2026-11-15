import { defineConfig } from 'tsdown'

// dsh-memory 构建：src/*.ts -> lib/*.js，逐文件镜像（不打包）。
// 两个硬约束别改：
//   - unbundle + root: 'src'：78 个测试直接 import lib/xxx.js，5 处 import.meta.dirname
//     相对路径按产物位置算；一旦并成 chunk，这些路径全失效。
//   - clean: false：lib/ 在刀 1 之前还混着手写源码，且插件 link 安装在跑、dsh-hmr
//     盯着 lib/index.js，clean 会先把入口删掉再重写。
export default defineConfig({
  entry: ['src/**/*.ts'],
  root: 'src',
  outDir: 'lib',
  format: 'esm',
  platform: 'node',
  target: 'node22',
  unbundle: true,
  clean: false,
  // 声明产物，刀 9 开启：@ts-nocheck 已清零、全仓 tsc 0 error，逐文件镜像出 .d.ts，
  // 对外发布的类型与 src 一致；Config 另行显式注解，避免推断类型带 pnpm 私有路径（TS2742）。
  dts: true,
  sourcemap: false,
  // platform: 'node' 时 tsdown 默认 fixedExtension: true，会产出 .mjs，
  // 而 main/exports/测试/相对路径全部认 .js，所以显式关掉。
  fixedExtension: false,
  // tsdown 0.22 起 external 已废弃，改用 deps.neverBundle（两者同时给会 TypeError）。
  deps: {
    neverBundle: [
      '@deepseek-ai/dsh-tools',
      '@deepseek-ai/schemastery',
      'sqlite-vec-linux-arm64',
    ],
  },
})
