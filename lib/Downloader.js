const moment = require("moment-timezone");
const fs = require("fs");
const minz = require("./MinZClient");
const config = require("./Config").getConfig();

class Downloader {
    constructor() {
        this.pathEstado = require("./Config").getConfig().dataPath + "/estado.json";
    }
    static get instance() {
        if (!Downloader.singleton) Downloader.singleton = new Downloader();
        return Downloader.singleton;
    }

    getEstado() {
        return new Promise((resolve, reject) => {
            fs.readFile(this.pathEstado, (err, data) => {
                if (err) {
                    if (err.code == "ENOENT") resolve(null);
                    else reject(err);
                } else resolve(JSON.parse(data));
            })
        });
    }
    setEstado(estado) {
        return new Promise((resolve, reject) => {
            fs.writeFile(this.pathEstado, JSON.stringify(estado), err => {
                if (err) reject(err);
                resolve();
            })
        })
    }

    start() {
        this.callDownload(1000);
    }
    callDownload(ms) {
        if (!ms) ms = 60000 * 5;
        if (this.timer) clearTimeout(this.timer);
        this.timer = setTimeout(_ => {
            this.timer = null;
            this.download()
        }, ms);
    }

    async download() {
        try {
            minz.setBatchOptions(300, {});
            if (!this.mareografos) {
                let path = global.resDir + "/mareografos.geojson";
                this.mareografos = JSON.parse(require("fs").readFileSync(path));
            }
            let estado = await this.getEstado();
            if (!estado) estado = {};
            for (let i=0; i<this.mareografos.features.length; i++) {
                let m = this.mareografos.features[i];
                console.log("Descargando mareografo:" + m.properties.codigo);
                let last = await this.downloadMareografo(m.properties.codigo, estado);
                if (last) {
                    estado[m.properties.codigo] = last;
                    await this.setEstado(estado);
                }
            }
            await minz.batchFlush();
        } catch(error) {
            console.error("Error en Downloader", error);
        } finally {
            this.callDownload();
        }
    }

    request(method, url, data) {
        const http = (url.toLowerCase().startsWith('https://') ? require("https"):require("http")) ;
        //process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;
        let postData, options = {method:method, headers:{}};
        if (this._token) options.headers.Authorization = "Bearer " + this._token;
        if (method == "POST") {
            postData = JSON.stringify(data || {});
            options.headers['Content-Type'] = 'application/json';
            options.headers['Content-Length'] = Buffer.byteLength(postData);            
        }
        return new Promise((resolve, reject) => {
            let req = http.request(url, options, res => {
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
 
    async downloadSensor(codigoSensor, codigoMareografo, estado) {
        try {
            
            let dt = 12;
            if (estado[codigoMareografo] && (Date.now - estado[codigoMareografo]) < 1000 * 60 * 60) dt = 1;
            let url = `
                http://wsprovimar.mitelemetria.cl/apps/src/ws/wsexterno.php?wsname=getData&idsensor=${codigoSensor}&idestacion=${codigoMareografo}&period=${dt}&fmt=json&tipo=tecmar&orden=ASC
            `;
            let datos = await this.request("GET", url, {});
            /*
            let path = global.resDir + "/sample.json";
            let datos = JSON.parse(fs.readFileSync(path));
            */
            if (!Array.isArray(datos)) {
                if (datos.result && datos.result.descripcion) throw datos.result.descripcion;
                throw datos;
            }
            if (!datos || !datos.length) return null;
            console.log(url);
            console.log("  => " + datos.length);
            let tiempoMayor = null;
            for (let i=0; i<datos.length; i++) {
                let d = datos[i];
                if (d.DATO !== null) {
                    let time = moment.tz(d.FECHA, "UTC");
                    if (!estado[codigoMareografo] || time.valueOf() > estado[codigoMareografo]) {
                        tiempoMayor = time.valueOf();
                        await minz.batchAccum("shoa.nivelMar", time.valueOf(), d.DATO / 1000, {
                            mareografo:codigoMareografo, sensor:codigoSensor
                        });
                    }
                }
            }
            if (tiempoMayor) {
                let m = moment.tz(tiempoMayor, config.timeZone);
                console.log("  => Actualizado a " + m.format("YYYY-MM-DD HH:mm"));
            } else {
                console.log("  => No actualiza");
            }
            return tiempoMayor;
        } catch(error) {
            throw error;
        }
    }
    async downloadMareografo(codigo, estado) {
        try {
            let tiempoMayor1 = await this.downloadSensor("PRS", codigo, estado);
            let tiempoMayor2 = await this.downloadSensor("RAD", codigo, estado);
            return tiempoMayor1 > tiempoMayor2?tiempoMayor1:tiempoMayor2;
        } catch(error) {
            console.error("Error descargando datos de mareografo " + codigo + ":", error);
        }
    }

    async importaSensor(codigoSensor, codigoMareografo, t0, t1) {
        try {
            let t1UTC = moment.tz(t1.valueOf(), "UTC");
            let fechaHora = t1UTC.format("YYYY-MM-DDTHH:mm");
            let url = `
                http://wsprovimar.mitelemetria.cl/apps/src/ws/wsexterno.php?wsname=getData&idsensor=${codigoSensor}&idestacion=${codigoMareografo}&date=${fechaHora}&period=48&fmt=json&tipo=tecmar&orden=ASC
            `;
            let datos = await this.request("GET", url, {});
            if (!Array.isArray(datos)) {
                if (datos.result && datos.result.descripcion) console.warn("   *** Error:" + datos.result.descripcion);
                else console.warn("   *** Error:", datos);
                return 0;
            }
            let n = 0;
            if (!datos || !datos.length) return 0;
            for (let i=0; i<datos.length; i++) {
                let d = datos[i];
                if (d.DATO !== null) {
                    let time = moment.tz(d.FECHA, "UTC").valueOf();
                    if (time >= t0.valueOf() && time < t1.valueOf()) {
                        await minz.batchAccum("shoa.nivelMar", time, d.DATO / 1000, {
                            mareografo:codigoMareografo, sensor:codigoSensor
                        });
                        n++;
                    }
                }
            }
            return n;
        } catch(error) {
            console.error(error);
            throw error;
        }
    }
    async importaPeriodo(t0, t1) {
        try {
            let n = 0;
            await minz.deletePeriod("shoa.nivelMar", t0.valueOf(), t1.valueOf(), true);
            if (!this.mareografos) {
                let path = global.resDir + "/mareografos.geojson";
                this.mareografos = JSON.parse(require("fs").readFileSync(path));
            }
            for (let i=0; i<this.mareografos.features.length; i++) {
                let m = this.mareografos.features[i];
                console.log("  - " + m.properties.nombre);
                n += await this.importaSensor("PRS", m.properties.codigo, t0, t1);
                n += await this.importaSensor("RAD", m.properties.codigo, t0, t1);
            }
            return n;
        } catch(error) {
            console.error(error);
            throw error;
        }
    }
    async importaHistoria(stFechaHora, n) {
        try {
            minz.setBatchOptions(300, {});
            let t1 = moment.tz(stFechaHora, config.timeZone);
            let t0 = t1.clone(); t0.hour(0); t0.minute(0); t0.second(0); t0.millisecond(0);
            if (t0.valueOf() == t1.valueOf()) t0.hours(t0.hours() - 48);
            let i=0;
            while(i < n) {
                console.log("Importando " + t0.format("DD/MM/YYYY HH:mm") + " - " + t1.format("DD/MM/YYYY HH:mm"));
                let total = await this.importaPeriodo(t0, t1);
                console.log("  => " + total + " valores importados");
                t1 = t0.clone();
                t0.hours(t0.hours() - 48);
                i++;
            }
        } catch(error) {
            console.error(error);
        }
    }
}

module.exports = Downloader.instance;