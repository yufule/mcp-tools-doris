/**
 * Doris 客户端类
 * 提供与 Doris 数据库交互的基本功能
 */
const mysql = require('mysql2/promise');
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');

class DorisClient {
  /**
   * 创建 Doris 客户端实例
   * @param {Object} config - 连接配置
   * @param {string} config.host - 主机地址
   * @param {number} config.port - 端口号
   * @param {string} config.user - 用户名
   * @param {string} config.password - 密码
   * @param {string} config.database - 数据库名
   * @param {number} config.timeout - 超时时间(毫秒)
   */
  constructor(config) {
    this.config = config;
    this.connection = null;
    this.isConnected = false;
  }

  /**
   * 连接到 Doris 数据库
   * @returns {Promise<boolean>} 连接是否成功
   */
  async connect() {
    try {
      this.connection = await mysql.createConnection({
        host: this.config.host,
        port: this.config.port,
        user: this.config.user,
        password: this.config.password,
        database: this.config.database,
        connectTimeout: this.config.timeout || 30000,
        // Doris 特定设置
        ssl: false,
        charset: 'utf8mb4',
      });
      
      this.isConnected = true;
      return true;
    } catch (error) {
      console.error('连接到 Doris 失败:', error.message);
      this.isConnected = false;
      throw error;
    }
  }

  /**
   * 断开与 Doris 的连接
   * @returns {Promise<void>}
   */
  async disconnect() {
    if (this.connection) {
      await this.connection.end();
      this.isConnected = false;
      this.connection = null;
    }
  }

  /**
   * 确保已连接到数据库
   * @private
   * @returns {Promise<void>}
   */
  async _ensureConnected() {
    if (!this.isConnected) {
      await this.connect();
    }
  }

  /**
   * 执行 SQL 查询
   * @param {string} sql - SQL 查询语句
   * @param {Array} params - 查询参数
   * @returns {Promise<Object>} 查询结果
   */
  async query(sql, params = []) {
    await this._ensureConnected();
    
    try {
      const [rows, fields] = await this.connection.execute(sql, params);
      return { rows, fields };
    } catch (error) {
      console.error('执行查询失败:', error.message);
      throw error;
    }
  }

  /**
   * 获取所有数据库列表
   * @returns {Promise<Array>} 数据库列表
   */
  async getDatabases() {
    const { rows } = await this.query('SHOW DATABASES');
    return rows.map(row => row.Database || row.name);
  }

  /**
   * 获取指定数据库中的所有表
   * @param {string} database - 数据库名
   * @returns {Promise<Array>} 表列表
   */
  async getTables(database) {
    const { rows } = await this.query(`SHOW TABLES FROM \`${database}\``);
    return rows.map(row => row.Tables_in_database || row.name);
  }

  /**
   * 获取表结构
   * @param {string} database - 数据库名
   * @param {string} table - 表名
   * @returns {Promise<Array>} 表结构信息
   */
  async getTableSchema(database, table) {
    await this._ensureConnected();
    const { rows } = await this.query(`DESC \`${database}\`.\`${table}\``);
    return rows;
  }

  /**
   * 通过 Doris HTTP 接口获取集群状态
   * @param {string} feHost - FE 主机地址
   * @param {number} fePort - FE HTTP 端口
   * @returns {Promise<Object>} 集群状态信息
   */
  async getClusterStatus(feHost, fePort) {
    try {
      const response = await axios.get(`http://${feHost}:${fePort}/api/cluster_status`);
      return response.data;
    } catch (error) {
      console.error('获取集群状态失败:', error.message);
      throw error;
    }
  }

  /**
   * 执行批量数据导入
   * @param {string} database - 数据库名
   * @param {string} table - 表名
   * @param {Array<Object>} data - 要导入的数据数组
   * @returns {Promise<Object>} 导入结果
   */
  async bulkImport(database, table, data) {
    if (!data || data.length === 0) {
      throw new Error('没有提供数据进行导入');
    }
    
    await this._ensureConnected();
    
    // 获取表结构以构建适当的 INSERT 语句
    const schema = await this.getTableSchema(database, table);
    const columns = schema.map(col => col.Field || col.name);
    
    // 构建批量插入语句
    const placeholders = columns.map(() => '?').join(', ');
    const sql = `INSERT INTO \`${database}\`.\`${table}\` (${columns.map(c => `\`${c}\``).join(', ')}) VALUES (${placeholders})`;
    
    // 处理数据行
    const values = data.map(row => {
      return columns.map(col => row[col]);
    });
    
    try {
      // MySQL2 bulk insert
      const [result] = await this.connection.query(sql, [values]);
      return {
        success: true,
        rowsAffected: result.affectedRows,
        message: `成功导入 ${result.affectedRows} 条数据`
      };
    } catch (error) {
      console.error('数据导入失败:', error.message);
      throw error;
    }
  }

  /**
   * 从文件导入数据
   * @param {string} database - 数据库名
   * @param {string} table - 表名
   * @param {string} filePath - 文件路径
   * @param {Object} options - 导入选项
   * @returns {Promise<Object>} 导入结果
   */
  async importFromFile(database, table, filePath, options = {}) {
    const fileExt = path.extname(filePath).toLowerCase();
    const loadSql = `LOAD LABEL \`${database}\`.${Date.now()} (
      DATA INFILE("${filePath}")
      INTO TABLE \`${table}\`
      ${options.columns ? `(${options.columns.join(', ')})` : ''}
      ${options.format || (fileExt === '.csv' ? 'FORMAT AS "CSV"' : 'FORMAT AS "ORC"')}
      ${options.columnSeparator ? `COLUMNS TERMINATED BY "${options.columnSeparator}"` : ''}
      ${options.where ? `WHERE ${options.where}` : ''}
    )`;
    
    try {
      const { rows } = await this.query(loadSql);
      return {
        success: true,
        result: rows,
        message: '导入任务已提交'
      };
    } catch (error) {
      console.error('文件导入失败:', error.message);
      throw error;
    }
  }

  /**
   * 导出数据到文件
   * @param {string} sql - 查询语句
   * @param {string} outputFile - 输出文件路径
   * @param {Object} options - 导出选项
   * @returns {Promise<Object>} 导出结果
   */
  async exportToFile(sql, outputFile, options = {}) {
    await this._ensureConnected();
    
    try {
      const { rows } = await this.query(sql);
      
      let content = '';
      const separator = options.separator || ',';
      
      // 如果需要导出标题行
      if (options.includeHeader && rows.length > 0) {
        content += Object.keys(rows[0]).join(separator) + '\n';
      }
      
      // 导出数据行
      for (const row of rows) {
        content += Object.values(row).join(separator) + '\n';
      }
      
      await fs.writeFile(outputFile, content, 'utf8');
      
      return {
        success: true,
        rowCount: rows.length,
        file: outputFile,
        message: `成功导出 ${rows.length} 条记录到 ${outputFile}`
      };
    } catch (error) {
      console.error('数据导出失败:', error.message);
      throw error;
    }
  }
}

module.exports = DorisClient;