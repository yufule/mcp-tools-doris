/**
 * Doris SQL查询工具
 * 用于通过MCP执行SQL查询
 */

const { DorisClient } = require('../src/index');
const { loadConfig } = require('../src/utils/helpers');

/**
 * 执行SQL查询并返回结果
 * @param {Object} params - 参数对象
 * @param {string} params.sql - 要执行的SQL查询语句
 * @param {string} [params.database] - 可选: 指定要在哪个数据库上执行查询
 * @returns {Promise<Object>} - 查询结果
 */
module.exports = async function dorisQuery(params) {
  try {
    const { sql, database } = params;
    
    // 加载配置
    const config = await loadConfig();
    
    // 创建Doris客户端
    const client = new DorisClient({
      ...config.doris,
      ...(database ? { database } : {})
    });
    
    // 连接到Doris
    await client.connect();
    
    // 执行查询
    const result = await client.query(sql);
    
    // 断开连接
    await client.disconnect();
    
    return {
      success: true,
      data: result.rows,
      message: `查询成功，返回 ${result.rows.length} 条记录`
    };
  } catch (error) {
    return {
      success: false,
      error: error.message,
      message: `查询失败: ${error.message}`
    };
  }
};