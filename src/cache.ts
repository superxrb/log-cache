import * as Loggers from "@litert/logger";
import * as Redis from "@litert/redis";

let logger = Loggers.default.createTextLogger("logger.cache");

const MAX_NUM = 1024;

const INTERVAL = 30000;
export class LogCache implements Loggers.IDriver {
    protected _service: string;
    protected _cacheClient!: Redis.RedisClient;
    
    // 当前本地内存中有多少条日志
    protected _quantity: number;

    // 内存中日志最大条数
    protected _bufferSize: number;
    protected _flushInterval: number;
    // 本地内存中的日志
    protected _logCache: Record<string, string[]>;
    // 上传刷新日志时间
    protected _lastFlushTime: number;
    public write(
        text: string, 
        subject: string, 
        level: string, 
        date: Date
    ) {

        let timestamp = parseInt((date.getTime() / 3600000).toString()) * 3600000;
        
        let cacheKey = `log_${this._service}_${subject}_${timestamp}`;

        this._logCache[cacheKey].push(text);        

        if (++ this._quantity >= this._bufferSize) {
            
            this.flush();
        }

    }

    /**
     * 将日志从本地内存刷到缓存Redis中
     */
    public async flush() {

        if (!this._cacheClient) {

            return;
        }

        let cachePromises = [];
        this._cacheClient.startPipeline();
        
        for (let key in this._logCache) {

            cachePromises.push(this._cacheClient.lPush(
                key,
                ...this._logCache[key]
            ));
        }
       
        this._cacheClient.endPipeline();
        this._logCache = {};
        this._quantity = 0;
        this._lastFlushTime = Date.now();
        try {
            await Promise.all(cachePromises);
        } catch (e) {
            logger.error(e);
        }
    }

    public close() {
       // 关闭时将本地内存日志刷到缓存Redis
       return this.flush();
    }

    protected async _tryFlush() {
       
        let date = Date.now();
       
        if (date - this._lastFlushTime >= this._flushInterval) {
            
            await this.flush();
        }
    }

    public setCacheClient(cacheClient: Redis.RedisClient) {
        
        if (this._cacheClient) {
            return;
        }
        
        this._cacheClient = cacheClient;
    }

    public constructor(
        serviceName: string,
        cacheClient?: Redis.RedisClient,
        bufferSize: number = MAX_NUM,
        flushInterval: number = INTERVAL
    ) {
        
        this._service = serviceName;

        if (cacheClient) {

            this._cacheClient = cacheClient;
        }
        this._bufferSize = bufferSize;
        this._flushInterval = flushInterval; 
        
        this._quantity = 0;
        this._logCache = {};
        this._lastFlushTime = 0;
        
        setInterval(this._tryFlush, this._flushInterval);
    }

}

export default LogCache;
