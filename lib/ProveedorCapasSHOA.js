const {ProveedorCapas, CapaObjetosConDatos} = require("geop-base-proveedor-capas");
const minz = require("./MinZClient");
const config = require("./Config").getConfig();
const turf = require("@turf/turf");

class ProveedorCapasSHOA extends ProveedorCapas {
    constructor(opciones) {
        super("shoa", opciones);
        this.addOrigen("shoa", "SHOA", "http://www.shoa.cl/", "./img/shoa.png");        

        let capaMareografos = new CapaObjetosConDatos("shoa", "Mareografos", "SHOA - Mareógrafos", "shoa", {
            temporal:false,
            datosDinamicos:false,
            menuEstaciones:true,
            dimensionMinZ:"shoa.mareografo",
            geoJSON:true,
            iconoEnMapa:"img/mareografo-shoa-2.png",
            configAnalisis:{
                analizador:"serie-tiempo",
                analizadores:{
                    "serie-tiempo":{
                        nivelVariable:0,
                        tiempo:{tipo:"relativo", from:-7, to:0, temporalidad:"1d"},
                        variable:{
                            variableMinZ:"shoa.nivelMar",
                            acumulador:"avg", 
                            codigo:"Nivel del Mar->mareografo",
                            filtroFijo:{ruta:"mareografo", valor:"${codigo-objeto}"},
                            filtros:[{ruta:"sensor", valor:"PRS"}],
                            icono:"${shoa}/img/nivel-mar.svg",
                            nombre:"Nivel del Mar",
                            temporalidad:"15m",
                            tipo:"queryMinZ"
                        },
                        variable2:{
                            variableMinZ:"shoa.nivelMar",
                            acumulador:"avg", 
                            codigo:"Nivel del Mar->mareografo",
                            filtroFijo:{ruta:"mareografo", valor:"${codigo-objeto}"},
                            filtros:[{ruta:"sensor", valor:"RAD"}],
                            icono:"${shoa}/img/nivel-mar.svg",
                            nombre:"Nivel del Mar",
                            temporalidad:"15m",
                            tipo:"queryMinZ"
                        }
                    }
                },
                width:300, height:280                
            }        
        }, ["cl-estaciones"], "img/mareografos.svg");
        this.addCapa(capaMareografos);   

        // cache
        this.mareografos = this.getFeaturesMareografos();
    }

    async resuelveConsulta(formato, args) {
        try {
            if (formato == "geoJSON") {
                return await this.generaGeoJSON(args);
            } else throw "Formato " + formato + " no soportado";
        } catch(error) {
            throw error;
        }
    }

    async generaGeoJSON(args) {
        try {           
            if (args.codigoVariable == "Mareografos") {
                return this.mareografos;
            } else throw "Código de Capa '" + args.codigoVariable + "' no manejado";            
        } catch(error) {
            throw error;
        }
    }
    
    getFeaturesMareografos() {
        try {
            let path = global.resDir + "/mareografos.geojson";
            let features = JSON.parse(require("fs").readFileSync(path));
            features.name = "SHOA - Mareografos";
            features.features.forEach(f => {
                f.properties.id = f.properties.codigo;
                f.properties._titulo = "Mareógrafo: " + f.properties.nombre;
            });
            console.log("[GEOOS - SHOA] Leidos " + features.features.length + " mareógrafos a cache");
            return features;
        } catch(error) {
            console.error("Error leyendo geojson de mareógrafos", error);
        }
    }

    // MinZ
    async comandoGET(cmd, req, res) {
        try {
            switch(cmd) {
                case "initMinZ":
                    this.initMinZ();
                    res.status(200).send("Corriendo en background ...").end();
                    break;
                case "preparaArchivos":
                    this.preparaArchivos();
                    res.status(200).send("Corriendo en background ...").end();
                    break;
                case "importaHistoria":
                    let fechaHora = req.query["fechahora"];
                    if (!fechaHora) throw "No se indicó fechahora (local)";
                    let n = parseInt(req.query["n"] || "10");
                    require("./Downloader").importaHistoria(fechaHora, n);
                    res.status(200).send("Importando en background ...").end();
                    break;
                default: throw "Comando '" + cmd + "' no implementado";
            }
        } catch(error) {
            console.error(error);
            if (typeof error == "string") {
                res.send(error).status(401).end();
            } else {
                res.send("Error Interno").status(500).end();
            }
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

    async initMinZ() {
        try {
            // Crear dimensiones            
            // Crear mareografos
            await minz.addOrSaveDimension({code:"shoa.mareografo", name:"Mareógrafo SHOA", classifiers:[
                {fieldName:"comuna", name:"Comuna", dimensionCode:"bcn.comuna", defaultValue:"00000"}
            ]});

            for (let i=0; i < this.mareografos.features.length; i++) {
                let m = this.mareografos.features[i];
                let codigoMareografo = m.properties.codigo;
                let codigoComuna = m.properties.codigoComuna;
                await minz.addOrUpdateRow("shoa.mareografo", {code:codigoMareografo, name:m.properties.nombre, comuna:codigoComuna});
            }

            // Sensores
            await minz.addOrSaveDimension({code:"shoa.sensorMareografo", name:"Sensor Mareógrafo", classifiers:[]});            
            await minz.addOrUpdateRow("shoa.sensorMareografo", {code:"PRS", name:"Sensor de Presión Hidrostática"});            
            await minz.addOrUpdateRow("shoa.sensorMareografo", {code:"RAD", name:"Sensor Radar"});                        

            // Crear Variables            
            await minz.addOrSaveVariable({
                code:"shoa.nivelMar",
                name:"Nivel del Mar",
                temporality:"5m",
                classifiers:[{
                    fieldName:"mareografo", name:"Mareógrafo", dimensionCode:"shoa.mareografo", defaultValue:"00"
                }, {
                    fieldName:"sensor", name:"Sensor", dimensionCode:"shoa.sensorMareografo", defaultValue:"0"
                }],
                options:{
                    unit:"m",
                    decimals:2,
                    defQuery:{
                        accum:"avg", temporality:"15m", filters:[
                            {path:"sensor", value:"PRS"}
                        ]
                    },
                    icon:"${shoa}/img/nivel-mar.svg"
                }
            });            
        } catch(error) {
            throw error;
        }
    }

    getComunaMasCercana(comunas, p, nombreMareografo) {
        // Buscar Comuna
        let comunaMasCercana, distanciaMasCercana, nombreComunaMasCercana;
        for (let j=0; j<comunas.length; j++) {
            let comuna = comunas[j];
            try {
                let poly = comuna.geometry.type == "Polygon"?turf.multiPolygon([comuna.geometry.coordinates]):turf.multiPolygon(comuna.geometry.coordinates);
                if (turf.booleanPointInPolygon(p, poly)) {
                    comunaMasCercana = comuna.properties._codigoDimension;
                    console.log(nombreMareografo + " [1] => " + comuna.properties.nombre);
                    break;
                }
            } catch(error) {
                console.warn(comuna.properties.nombre + ":" + error.toString());
            }
        }
        if (!comunaMasCercana) {
            for (let j=0; j<comunas.length; j++) {
                let comuna = comunas[j];
                let poly = comuna.geometry.type == "Polygon"?turf.multiPolygon([comuna.geometry.coordinates]):turf.multiPolygon(comuna.geometry.coordinates);
                try {
                    let vertices = turf.explode(poly)
                    let closestVertex = turf.nearest(p, vertices)
                    let distance = turf.distance(p, closestVertex);
                    //console.log(comuna.geometry.type, distance);
                    if (!comunaMasCercana || distance < distanciaMasCercana) {
                        comunaMasCercana = comuna.properties._codigoDimension;
                        distanciaMasCercana = distance;
                        nombreComunaMasCercana = comuna.properties.nombre;
                    }
                } catch(error) {
                    console.warn(comuna.geometry.type, error);
                }
            }
            console.log(nombreMareografo + " [2] => " + nombreComunaMasCercana);
        }
        return comunaMasCercana;
    }

    async preparaArchivos() {
        try {
            let geojson = {
                type:"FeatureCollection",
                name:"SHOA - Mareógrafos",
                crs:{type:"name", properties:{name:"urn:ogc:def:crs:OGC:1.3:CRS84"}},
                features:[]
            }
            // Obtener comunas para asociar estaciones
            let url = config.bcnUrl + "/consulta";
            let args = {
                formato:"geoJSON", 
                args:{codigoVariable:"Comunas"}
            }
            console.log("buscando comunas");
            let comunas = (await this.request("POST", url, args)).features;
            console.log("comunas", comunas.length);

            const fs = require("fs");
            let path = global.resDir + "/mareografos.csv";
            let csv = fs.readFileSync(path).toString().split("\n");
            console.log("csv:" + csv.length);
            for (let i=1; i<csv.length; i++) {
                let fields = csv[i].split(",");
                if (fields.length != 7) continue;
                let codigo = fields[0]; 
                let nombre = fields[1];
                let lat = parseFloat(fields[5]);
                let lng = parseFloat(fields[6]);
                let feature = {
                    type:"Feature",
                    properties:{
                        id:codigo, nombre:nombre, codigo:codigo,
                        _codigoDimension:codigo,
                        codigoComuna:this.getComunaMasCercana(comunas, turf.point([lng, lat]), nombre)
                    },
                    geometry:{
                        type:"Point",
                        coordinates:[lng, lat]
                    }
                }
                geojson.features.push(feature);
            }
            fs.writeFileSync(global.resDir + "/mareografos.geojson", JSON.stringify(geojson));
        } catch(error) {
            console.error(error);
            throw error;
        }
    }
}

module.exports = ProveedorCapasSHOA;