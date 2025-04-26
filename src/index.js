/**
 * MCP Doris 工具主入口
 * 导出所有模块供API使用
 */

const DorisClient = require('./lib/client');
const DorisManager = require('./lib/manager');
const helpers = require('./utils/helpers');

/**
 * 创建 Doris 客户端实例
 * @param {Object} config - 连接配置
 * @returns {DorisClient} Doris客户端实例
 */
function createClient(config) {
  return new DorisClient(config);
}

/**
 * 创建 Doris 管理器实例
 * @param {Object} config - 配置信息
 * @returns {DorisManager} Doris管理器实例
 */
function createManager(config) {
  return new DorisManager(config);
}

/**
 * 从配置文件加载并创建 Doris 客户端和管理器
 * @param {string} configPath - 配置文件路径
 * @returns {Promise<Object>} 包含客户端和管理器的对象
 */
async function createFromConfig(configPath) {
  const config = await helpers.loadConfig(configPath);
  
  return {
    client: new DorisClient(config.doris),
    manager: new DorisManager(config)
  };
}

// 导出所有模块
module.exports = {
  DorisClient,
  DorisManager,
  helpers,
  createClient,
  createManager,
  createFromConfig
};