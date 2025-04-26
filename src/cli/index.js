#!/usr/bin/env node

/**
 * Doris CLI 工具入口
 */
const { program } = require('commander');
const inquirer = require('inquirer');
const ora = require('ora');
const chalk = require('chalk');
const path = require('path');

const { loadConfig, formatTable } = require('../utils/helpers');
const DorisClient = require('../lib/client');
const DorisManager = require('../lib/manager');

let config;
let client;
let manager;

/**
 * 初始化配置和客户端
 */
async function init() {
  try {
    config = await loadConfig();
    client = new DorisClient(config.doris);
    manager = new DorisManager(config);
    return true;
  } catch (error) {
    console.error(chalk.red('初始化失败:'), error.message);
    return false;
  }
}

/**
 * 执行查询命令
 */
async function executeQuery(sql, options) {
  const spinner = ora('执行查询中...').start();
  
  try {
    const { rows } = await client.query(sql);
    spinner.succeed('查询完成');
    
    if (rows.length > 0) {
      const headers = Object.keys(rows[0]);
      console.log(formatTable(rows, headers));
      
      if (options.output) {
        const fs = require('fs').promises;
        const { exportToCSV } = require('../utils/helpers');
        const csvContent = exportToCSV(rows);
        await fs.writeFile(options.output, csvContent, 'utf8');
        console.log(chalk.green(`结果已保存到 ${options.output}`));
      }
    } else {
      console.log(chalk.yellow('查询结果为空'));
    }
  } catch (error) {
    spinner.fail('查询失败');
    console.error(chalk.red('错误:'), error.message);
  }
}

/**
 * 显示集群状态
 */
async function showClusterStatus() {
  const spinner = ora('获取集群状态中...').start();
  
  try {
    const status = await manager.getClusterStatus();
    spinner.succeed('获取集群状态成功');
    
    // 显示 FE 节点信息
    if (status.frontends && status.frontends.length > 0) {
      console.log(chalk.blue.bold('\nFE 节点:'));
      const feHeaders = ['名称', '主机', '角色', '状态', '版本'];
      const feData = status.frontends.map(fe => [
        fe.name,
        `${fe.host}:${fe.edit_log_port}`,
        fe.role,
        fe.alive ? chalk.green('在线') : chalk.red('离线'),
        fe.version || '-'
      ]);
      console.log(formatTable(feData, feHeaders));
    }
    
    // 显示 BE 节点信息
    if (status.backends && status.backends.length > 0) {
      console.log(chalk.blue.bold('\nBE 节点:'));
      const beHeaders = ['ID', '主机', '状态', '数据目录数', '总磁盘容量', '可用磁盘容量'];
      const beData = status.backends.map(be => [
        be.be_id,
        `${be.host}:${be.heartbeat_port}`,
        be.alive ? chalk.green('在线') : chalk.red('离线'),
        be.data_dir_count || '-',
        be.total_capacity || '-',
        be.available_capacity || '-'
      ]);
      console.log(formatTable(beData, beHeaders));
    }
  } catch (error) {
    spinner.fail('获取集群状态失败');
    console.error(chalk.red('错误:'), error.message);
  }
}

/**
 * 显示数据库列表
 */
async function showDatabases() {
  const spinner = ora('获取数据库列表中...').start();
  
  try {
    const databases = await client.getDatabases();
    spinner.succeed('获取数据库列表成功');
    
    console.log(chalk.blue.bold('\n数据库列表:'));
    const data = databases.map(db => [db]);
    console.log(formatTable(data, ['数据库名']));
  } catch (error) {
    spinner.fail('获取数据库列表失败');
    console.error(chalk.red('错误:'), error.message);
  }
}

/**
 * 显示表列表
 */
async function showTables(database) {
  const spinner = ora(`获取 ${database} 中的表列表...`).start();
  
  try {
    const tables = await client.getTables(database);
    spinner.succeed(`获取 ${database} 中的表列表成功`);
    
    console.log(chalk.blue.bold(`\n${database} 中的表列表:`));
    const data = tables.map(table => [table]);
    console.log(formatTable(data, ['表名']));
  } catch (error) {
    spinner.fail(`获取 ${database} 中的表列表失败`);
    console.error(chalk.red('错误:'), error.message);
  }
}

/**
 * 显示表结构
 */
async function showTableSchema(database, table) {
  const spinner = ora(`获取 ${database}.${table} 的结构...`).start();
  
  try {
    const schema = await client.getTableSchema(database, table);
    spinner.succeed(`获取 ${database}.${table} 的结构成功`);
    
    console.log(chalk.blue.bold(`\n${database}.${table} 的结构:`));
    const headers = ['字段', '类型', '空', '键', '默认值', '额外信息'];
    console.log(formatTable(schema, headers));
  } catch (error) {
    spinner.fail(`获取 ${database}.${table} 的结构失败`);
    console.error(chalk.red('错误:'), error.message);
  }
}

/**
 * 显示运行中的查询
 */
async function showProcesslist() {
  const spinner = ora('获取运行中的查询...').start();
  
  try {
    const processes = await manager.getRunningQueries();
    spinner.succeed('获取运行中的查询成功');
    
    console.log(chalk.blue.bold('\n运行中的查询:'));
    const headers = ['ID', '用户', '主机', '数据库', '命令', '时间', '状态', 'Info'];
    console.log(formatTable(processes, headers));
  } catch (error) {
    spinner.fail('获取运行中的查询失败');
    console.error(chalk.red('错误:'), error.message);
  }
}

/**
 * 导入数据
 */
async function importData(file, database, table, options) {
  const spinner = ora(`导入数据到 ${database}.${table}...`).start();
  
  try {
    const result = await client.importFromFile(database, table, file, options);
    spinner.succeed(`导入任务已提交: ${result.message}`);
  } catch (error) {
    spinner.fail('导入数据失败');
    console.error(chalk.red('错误:'), error.message);
  }
}

/**
 * 导出数据
 */
async function exportData(sql, outputFile, options) {
  const spinner = ora('导出数据中...').start();
  
  try {
    const result = await client.exportToFile(sql, outputFile, options);
    spinner.succeed(`导出完成: ${result.message}`);
  } catch (error) {
    spinner.fail('导出数据失败');
    console.error(chalk.red('错误:'), error.message);
  }
}

/**
 * 交互式 SQL 终端
 */
async function startInteractiveTerminal() {
  console.log(chalk.blue.bold('Doris 交互式终端 (输入 exit 或 quit 退出)'));
  console.log(chalk.yellow('连接信息:'), `${config.doris.user}@${config.doris.host}:${config.doris.port}/${config.doris.database}`);
  
  let running = true;
  
  while (running) {
    const { sql } = await inquirer.prompt([
      {
        type: 'input',
        name: 'sql',
        message: 'doris> ',
        prefix: ''
      }
    ]);
    
    const command = sql.trim().toLowerCase();
    
    if (command === 'exit' || command === 'quit') {
      running = false;
      console.log(chalk.green('再见!'));
    } else if (command) {
      await executeQuery(sql, {});
    }
  }
}

// 定义命令行程序
program
  .name('doris-cli')
  .description('Doris 命令行工具')
  .version('1.0.0');

// 查询命令
program
  .command('query <sql>')
  .description('执行 SQL 查询')
  .option('-o, --output <file>', '将结果保存到文件')
  .action(async (sql, options) => {
    if (await init()) {
      await executeQuery(sql, options);
      await client.disconnect();
    }
  });

// 集群状态命令
program
  .command('status')
  .description('显示集群状态')
  .action(async () => {
    if (await init()) {
      await showClusterStatus();
    }
  });

// 数据库命令
program
  .command('databases')
  .description('显示所有数据库')
  .action(async () => {
    if (await init()) {
      await showDatabases();
      await client.disconnect();
    }
  });

// 表命令
program
  .command('tables <database>')
  .description('显示数据库中的所有表')
  .action(async (database) => {
    if (await init()) {
      await showTables(database);
      await client.disconnect();
    }
  });

// 表结构命令
program
  .command('schema <database> <table>')
  .description('显示表结构')
  .action(async (database, table) => {
    if (await init()) {
      await showTableSchema(database, table);
      await client.disconnect();
    }
  });

// 进程列表命令
program
  .command('processlist')
  .description('显示运行中的查询')
  .action(async () => {
    if (await init()) {
      await showProcesslist();
      await client.disconnect();
    }
  });

// 导入命令
program
  .command('import <file> <database> <table>')
  .description('导入数据到表')
  .option('-f, --format <format>', '文件格式 (CSV, JSON, ORC)')
  .option('-s, --separator <char>', '列分隔符')
  .option('-c, --columns <columns>', '列名列表，逗号分隔')
  .action(async (file, database, table, options) => {
    if (await init()) {
      const importOptions = {
        format: options.format,
        columnSeparator: options.separator,
        columns: options.columns ? options.columns.split(',') : undefined
      };
      await importData(file, database, table, importOptions);
      await client.disconnect();
    }
  });

// 导出命令
program
  .command('export <sql> <outputFile>')
  .description('导出查询结果到文件')
  .option('-s, --separator <char>', '列分隔符')
  .option('-h, --no-header', '不包含表头')
  .action(async (sql, outputFile, options) => {
    if (await init()) {
      const exportOptions = {
        separator: options.separator || ',',
        includeHeader: options.header !== false
      };
      await exportData(sql, outputFile, exportOptions);
      await client.disconnect();
    }
  });

// 交互式终端命令
program
  .command('shell')
  .description('启动交互式 SQL 终端')
  .action(async () => {
    if (await init()) {
      await startInteractiveTerminal();
      await client.disconnect();
    }
  });

// 解析命令行参数
program.parse();