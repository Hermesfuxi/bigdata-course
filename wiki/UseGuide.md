# 项目文档 wiki GitBook 使用
## 一、复制项目，并修改文档中项目名称

全局替换项目名(注意项目名命名规范，不用有一些奇怪的字符，如"c++"之类)

## 二、执行命令
```shell script
nvm use 13.5.0
nvm list

# 没有安装全局依赖的先安装
npm install -g gitbook-cli
gitbook -v

npm install -g gh-pages
gh-pages -v

cd wiki
npm install
gitbook install  # 安装比较慢，本地有同类项目可直接复制依赖过去

gitbook build
gitbook serve # 端口被占用的话，需要添加 --port 12333 之类

# 推送到 gh-pages 分支
npm run deploy
```
