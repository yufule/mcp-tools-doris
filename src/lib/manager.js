/**
 * Doris 管理类
 * 提供 Doris 集群管理和监控功能
 */
const axios = require('axios');
const DorisClient = require('./client');

class DorisManager {
  /**
   * 创建 Doris 管理器实例
   * @param {Object} config - 配置信息
   * @param {Object} config.fe - FE节点配置
   * @param {Array} config.be - BE节点配置列表
   */
  constructor(config) {
    this.config = config;
    this.client = new DorisClient(config.doris);
  }

  /**
   * 获取集群节点状态
   * @returns {Promise<Object>} 集群状态信息
   */
  async getClusterStatus() {
    try {
      const { fe } = this.config;
      const url = `http://${fe.host}:${fe.httpPort}/api/cluster_status`;
      
      const response = await axios.get(url, {
        auth: {
          username: this.config.doris.user,
          password: this.config.doris.password
        }
      });
      
      return response.data;
    } catch (error) {
      console.error('获取集群状态失败:', error.message);
      throw error;
    }
  }

  /**
   * 获取 FE 节点列表
   * @returns {Promise<Array>} FE节点列表
   */
  async getFeNodes() {
    try {
      const clusterStatus = await this.getClusterStatus();
      return clusterStatus.frontends || [];
    } catch (error) {
      console.error('获取FE节点失败:', error.message);
      throw error;
    }
  }

  /**
   * 获取 BE 节点列表
   * @returns {Promise<Array>} BE节点列表
   */
  async getBeNodes() {
    try {
      const clusterStatus = await this.getClusterStatus();
      return clusterStatus.backends || [];
    } catch (error) {
      console.error('获取BE节点失败:', error.message);
      throw error;
    }
  }

  /**
   * 获取查询进度
   * @param {string} queryId - 查询ID
   * @returns {Promise<Object>} 查询进度信息
   */
  async getQueryProgress(queryId) {
    try {
      const { fe } = this.config;
      const url = `http://${fe.host}:${fe.httpPort}/api/query/${queryId}/profile`;
      
      const response = await axios.get(url, {
        auth: {
          username: this.config.doris.user,
          password: this.config.doris.password
        }
      });
      
      return response.data;
    } catch (error) {
      console.error('获取查询进度失败:', error.message);
      throw error;
    }
  }

  /**
   * 获取表分区信息
   * @param {string} database - 数据库名
   * @param {string} table - 表名
   * @returns {Promise<Array>} 分区信息
   */
  async getTablePartitions(database, table) {
    try {
      await this.client.connect();
      const { rows } = await this.client.query(`SHOW PARTITIONS FROM \`${database}\`.\`${table}\``);
      return rows;
    } catch (error) {
      console.error('获取表分区信息失败:', error.message);
      throw error;
    }
  }

  /**
   * 获取表统计信息
   * @param {string} database - 数据库名
   * @param {string} table - 表名
   * @returns {Promise<Object>} 表统计信息
   */
  async getTableStats(database, table) {
    try {
      await this.client.connect();
      const { rows } = await this.client.query(`SHOW STATS \`${database}\`.\`${table}\``);
      return rows[0];
    } catch (error) {
      console.error('获取表统计信息失败:', error.message);
      throw error;
    }
  }

  /**
   * 获取集群资源使用情况
   * @returns {Promise<Object>} 资源使用情况
   */
  async getResourceUsage() {
    try {
      const { fe } = this.config;
      const url = `http://${fe.host}:${fe.httpPort}/api/system/metrics`;
      
      const response = await axios.get(url, {
        auth: {
          username: this.config.doris.user,
          password: this.config.doris.password
        }
      });
      
      return response.data;
    } catch (error) {
      console.error('获取资源使用情况失败:', error.message);
      throw error;
    }
  }

  /**
   * 查看正在运行的查询
   * @returns {Promise<Array>} 运行中的查询列表
   */
  async getRunningQueries() {
    try {
      await this.client.connect();
      const { rows } = await this.client.query('SHOW PROCESSLIST');
      return rows;
    } catch (error) {
      console.error('获取运行中的查询失败:', error.message);
      throw error;
    }
  }

  /**
   * 终止正在运行的查询
   * @param {string} queryId - 查询ID
   * @returns {Promise<boolean>} 是否成功
   */
  async killQuery(queryId) {
    try {
      await this.client.connect();
      await this.client.query(`KILL '${queryId}'`);
      return true;
    } catch (error) {
      console.error('终止查询失败:', error.message);
      throw error;
    }
  }

  /**
   * 获取Doris版本信息
   * @returns {Promise<string>} 版本信息
   */
  async getVersion() {
    try {
      await this.client.connect();
      const { rows } = await this.client.query('SELECT DORIS_VERSION() as version');
      return rows[0].version;
    } catch (error) {
      console.error('获取版本信息失败:', error.message);
      throw error;
    }
  }

  /**
   * 重启 FE 节点
   * @param {string} host - FE主机地址
   * @param {number} port - FE HTTP端口
   * @returns {Promise<boolean>} 是否成功
   */
  async restartFeNode(host, port) {
    try {
      const url = `http://${host}:${port}/api/admin/restart`;
      
      await axios.post(url, {}, {
        auth: {
          username: this.config.doris.user,
          password: this.config.doris.password
        }
      });
      
      return true;
    } catch (error) {
      console.error('重启FE节点失败:', error.message);
      throw error;
    }
  }

  /**
   * 添加 BE 节点
   * @param {string} host - BE主机地址
   * @param {number} port - BE Heartbeat端口
   * @returns {Promise<boolean>} 是否成功
   */
  async addBeNode(host, port) {
    try {
      await this.client.connect();
      await this.client.query(`ALTER SYSTEM ADD BACKEND "${host}:${port}"`);
      return true;
    } catch (error) {
      console.error('添加BE节点失败:', error.message);
      throw error;
    }
  }

  /**
   * 移除 BE 节点
   * @param {string} host - BE主机地址
   * @param {number} port - BE Heartbeat端口
   * @returns {Promise<boolean>} 是否成功
   */
  async removeBeNode(host, port) {
    try {
      await this.client.connect();
      await this.client.query(`ALTER SYSTEM DROP BACKEND "${host}:${port}"`);
      return true;
    } catch (error) {
      console.error('移除BE节点失败:', error.message);
      throw error;
    }
  }
}

module.exports = DorisManager;