const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');

class ConfigLoader {
    constructor() {
        this.config = null;
        this.loadEnv();
    }

    loadEnv() {
        // 首先尝试加载 .env 文件
        dotenv.config();
        
        // 然后尝试加载 mcp.json
        const mcpPath = path.resolve(process.cwd(), 'mcp.json');
        if (fs.existsSync(mcpPath)) {
            try {
                const mcpConfig = JSON.parse(fs.readFileSync(mcpPath, 'utf8'));
                if (mcpConfig.doris && mcpConfig.doris.env) {
                    // 使用 mcp.json 中的环境变量覆盖 .env 中的配置
                    Object.assign(process.env, this.resolveEnvVariables(mcpConfig.doris.env));
                }
            } catch (error) {
                console.warn('无法加载 mcp.json 配置文件:', error.message);
            }
        }
    }

    resolveEnvVariables(envConfig) {
        const resolved = {};
        for (const [key, value] of Object.entries(envConfig)) {
            if (typeof value === 'string' && value.startsWith('${') && value.endsWith('}')) {
                // 替换环境变量占位符
                const envKey = value.slice(2, -1);
                resolved[key] = process.env[envKey] || '';
            } else {
                resolved[key] = value;
            }
        }
        return resolved;
    }

    getDorisConfig() {
        return {
            host: process.env.DORIS_HOST || 'localhost',
            port: parseInt(process.env.DORIS_PORT || '9030'),
            user: process.env.DORIS_USER || 'root',
            password: process.env.DORIS_PASSWORD || ''
        };
    }
}

module.exports = new ConfigLoader();