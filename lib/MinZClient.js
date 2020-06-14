const config = require("./Config").getConfig();

class MinZClient {
    constructor() {
        this._url = config.minZUrl;
        this._token = config.minZAuthToken;
    }
    static get instance() {
        if (!MinZClient.singleton) MinZClient.singleton = new MinZClient();
        return MinZClient.singleton;
    }

    request(method, url, data) {
        const http = (this._url.toLowerCase().startsWith('https://') ? require("https"):require("http")) ;
        //process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;
        let postData, options = {method:method, headers:{}};
        if (this._token) options.headers.Authorization = "Bearer " + this._token;
        if (method == "POST") {
            postData = JSON.stringify(data || {});
            options.headers['Content-Type'] = 'application/json';
            options.headers['Content-Length'] = Buffer.byteLength(postData);            
        }
        return new Promise((resolve, reject) => {
            let req = http.request(this._url + "/" + url, options, res => {
                let chunks = [];
                res.on("data", chunk => chunks.push(chunk));
                res.on("end", _ => {
                    let body = Buffer.concat(chunks).toString();
                    if (res.statusCode != 200) reject(body);
                    else resolve(JSON.parse(body));
                });
            });
            req.on("error", err => reject(err));
            if (method == "POST") req.write(postData);
            req.end();
        }); 
    }
    
    // Dimensiones
    async getDimensions() {
        try {
            return await this.request("GET", "dim/dimensions");
        } catch(error) {
            throw error;
        }
    }
    async getDimension(code) {
        try {
            return await this.request("GET", "dim/" + code);
        } catch(error) {
            throw error;
        }
    }
    async existeDimension(code) {
        try {
            return (await this.getDimension(code))?true:false;
        } catch(error) {
            return false;
        }
    }
    async addOrSaveDimension(dimension) {
        try {
            return await this.request("POST", "dim", dimension);
        } catch(error) {
            throw error;
        }
    }
    async deleteDimension(code) {
        try {
            return await this.request("DELETE", "dim/" + code);
        } catch(error) {
            throw error;
        }
    }

    // Filas
    async addOrUpdateRow(dimCode, row) {
        try {
            return await this.request("POST", "dim/" + dimCode + "/rows", row);
        } catch(error) {
            throw error;
        }
    }
    async getRow(dimCode, rowCode) {
        try {
            return await this.request("GET", "dim/" + dimCode + "/rows/" + rowCode);
        } catch(error) {
            throw error;
        }
    }
    async deleteRow(dimCode, rowCode) {
        try {
            return await this.request("DELETE", "dim/" + dimCode + "/rows/" + rowCode);
        } catch(error) {
            throw error;
        }
    }
    async findRows(dimCode, textFilter, filter, startRow, nRows, includeNames) {
        try {
            let url = "dim/" + dimCode + "/rows";
            filter = filter || {};
            url += "?filter=" + encodeURIComponent(JSON.stringify(filter));
            if (textFilter) url += "&textFilter=" + encodeURIComponent(textFilter);
            if (startRow && nRows) url += "&startRow=" + encodeURIComponent(startRow) + "&nRows=" + encodeURIComponent(nRows);
            if (includeNames) url += "&includeNames=true";
            return await this.request("GET", url);
        } catch(error) {
            throw error;
        }
    }

    // Variables
    async getVariables() {
        try {
            return await this.request("GET", "var/variables");
        } catch(error) {
            throw error;
        }
    }
    async getVariable(code) {
        try {
            return await this.request("GET", "var/" + code);
        } catch(error) {
            throw error;
        }
    }
    async existeVariable(code) {
        try {
            return (await this.getVariable(code))?true:false;
        } catch(error) {
            return false;
        }
    }
    async addOrSaveVariable(variable) {
        try {
            return await this.request("POST", "var", variable);
        } catch(error) {
            throw error;
        }
    }
    async deleteVariable(code) {
        try {
            return await this.request("DELETE", "var/" + code);
        } catch(error) {
            throw error;
        }
    }

    // Datos
    async deletePeriod(varCode, startTime, endTime, varData, details) {
        try {
            let url = `data/${varCode}/period?startTime=${startTime}&endTime=${endTime}`;
            if (varData) url += `&varData=${varData?"true":"false"}`;
            if (details) url += `&details=${details?"true":"false"}`;
            return await this.request("DELETE", url);
        } catch(error) {
            throw error;
        }
    }
    async postData(varCode, time, value, data, options) {
        try {
            return await this.request("POST", "data/" + varCode, {time:time, value:value, data:data, options:options});
        } catch(error) {
            throw error;
        }
    }
    setBatchOptions(size, options) {
        this.batchSize = size;
        this.batchOptions = options;
    }
    async batchAccum(varCode, time, value, data) {
        try {
            if (this.batchSize === undefined) throw "No batch options set";
            if (!this.batch) this.batch = [];
            this.batch.push({variable:varCode, time:time, value:value,data:data});
            if (this.batch.length >= this.batchSize) {
                await this.batchFlush();
            }
        } catch(error) {
            throw error;
        }
    }
    async batchFlush() {
        try {
            if (!this.batch || !this.batch.length) return;
            //console.log("*** [MinZ] Flushing " + this.batch.length + " records");
            await this.request("POST", "batch", {batch:this.batch, options:this.batchOptions});
            this.batch = [];
        } catch(error) {
            throw error;
        }
    }
}

module.exports = MinZClient.instance;