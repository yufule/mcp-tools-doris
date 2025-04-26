/**
 * 工具函数库
 * 提供各种辅助函数
 */
const fs = require('fs').promises;
const path = require('path');
const { table } = require('table');
const chalk = require('chalk');

/**
 * 加载配置文件
 * @param {string} configPath - 配置文件路径
 * @returns {Promise<Object>} 配置对象
 */
async function loadConfig(configPath = path.join(process.cwd(), 'config.json')) {
  try {
    const configData = await fs.readFile(configPath, 'utf8');
    return JSON.parse(configData);
  } catch (error) {
    console.error('加载配置文件失败:', error.message);
    throw error;
  }
}

/**
 * 格式化表格数据并打印
 * @param {Array} data - 表格数据
 * @param {Array} headers - 表格标题
 * @returns {string} 格式化后的表格
 */
function formatTable(data, headers) {
  // 添加标题行
  const tableData = [headers.map(h => chalk.bold(h))];
  
  // 添加数据行
  data.forEach(row => {
    // 如果行是对象，按照标题顺序提取值
    if (typeof row === 'object' && !Array.isArray(row)) {
      const rowValues = headers.map(header => {
        const value = row[header] || '';
        return value;
      });
      tableData.push(rowValues);
    } else if (Array.isArray(row)) {
      // 如果行已经是数组，直接添加
      tableData.push(row);
    }
  });
  
  return table(tableData);
}

/**
 * 格式化字节大小
 * @param {number} bytes - 字节数
 * @param {number} decimals - 小数位数
 * @returns {string} 格式化后的大小
 */
function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes';
  
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
  
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

/**
 * 格式化日期时间
 * @param {string|Date} date - 日期对象或字符串
 * @returns {string} 格式化后的日期时间
 */
function formatDateTime(date) {
  const d = new Date(date);
  
  return d.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
}

/**
 * 解析 CSV 字符串
 * @param {string} csvString - CSV 字符串
 * @param {string} delimiter - 分隔符
 * @returns {Array<Array>} 解析后的数据
 */
function parseCSV(csvString, delimiter = ',') {
  const lines = csvString.split('\n');
  return lines.map(line => line.split(delimiter));
}

/**
 * 将对象数组导出为 CSV 字符串
 * @param {Array<Object>} data - 数据对象数组
 * @param {string} delimiter - 分隔符
 * @returns {string} CSV 字符串
 */
function exportToCSV(data, delimiter = ',') {
  if (!data || data.length === 0) return '';
  
  // 获取标题（第一个对象的所有键）
  const headers = Object.keys(data[0]);
  
  // 添加标题行
  let csvContent = headers.join(delimiter) + '\n';
  
  // 添加数据行
  data.forEach(item => {
    const row = headers.map(header => {
      // 处理可能包含分隔符或换行符的值
      let cell = item[header] || '';
      if (cell === null || cell === undefined) cell = '';
      cell = String(cell);
      
      // 如果包含分隔符、引号或换行符，用引号括起来
      if (cell.includes(delimiter) || cell.includes('"') || cell.includes('\n')) {
        cell = `"${cell.replace(/"/g, '""')}"`;
      }
      
      return cell;
    });
    
    csvContent += row.join(delimiter) + '\n';
  });
  
  return csvContent;
}

/**
 * 简单的进度显示
 * @param {number} current - 当前进度
 * @param {number} total - 总数
 * @param {number} width - 进度条宽度
 * @returns {string} 进度条字符串
 */
function progressBar(current, total, width = 30) {
  const percentage = Math.round((current / total) * 100);
  const completedWidth = Math.round((current / total) * width);
  const remaining = width - completedWidth;
  
  const completed = '='.repeat(completedWidth);
  const empty = ' '.repeat(remaining);
  
  return `[${completed}>${empty}] ${percentage}% (${current}/${total})`;
}

/**
 * 读取输入文件并返回内容
 * @param {string} filePath - 文件路径
 * @returns {Promise<string>} 文件内容
 */
async function readInputFile(filePath) {
  try {
    return await fs.readFile(filePath, 'utf8');
  } catch (error) {
    console.error('读取文件失败:', error.message);
    throw error;
  }
}

/**
 * 写入输出文件
 * @param {string} filePath - 文件路径
 * @param {string} content - 文件内容
 * @returns {Promise<void>}
 */
async function writeOutputFile(filePath, content) {
  try {
    // 确保目录存在
    const dir = path.dirname(filePath);
    await fs.mkdir(dir, { recursive: true });
    
    // 写入文件
    await fs.writeFile(filePath, content, 'utf8');
  } catch (error) {
    console.error('写入文件失败:', error.message);
    throw error;
  }
}

/**
 * 解析命令行参数
 * @param {Array} args - 命令行参数数组
 * @returns {Object} 解析后的参数对象
 */
function parseArgs(args) {
  const params = {};
  let currentKey = null;
  
  args.forEach(arg => {
    if (arg.startsWith('--')) {
      // 长格式参数 (--param=value 或 --param value)
      const parts = arg.substring(2).split('=');
      currentKey = parts[0];
      
      if (parts.length > 1) {
        params[currentKey] = parts[1];
        currentKey = null;
      } else {
        params[currentKey] = true;
      }
    } else if (arg.startsWith('-')) {
      // 短格式参数 (-p value)
      currentKey = arg.substring(1);
      params[currentKey] = true;
    } else if (currentKey) {
      // 参数值
      params[currentKey] = arg;
      currentKey = null;
    }
  });
  
  return params;
}

module.exports = {
  loadConfig,
  formatTable,
  formatBytes,
  formatDateTime,
  parseCSV,
  exportToCSV,
  progressBar,
  readInputFile,
  writeOutputFile,
  parseArgs
};