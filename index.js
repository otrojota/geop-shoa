global.confPath = __dirname + "/config.json";
global.resDir = __dirname + "/resources";
const config = require("./lib/Config").getConfig();
const ProveedorCapasSHOA = require("./lib/ProveedorCapasSHOA");

let downloader = false;
for (let i=2; i<process.argv.length; i++) {
    let arg = process.argv[i].toLowerCase();
    if (arg == "-d" || arg == "-download" || arg == "-downloader") downloader = true;
}
if (!downloader && process.env.DOWNLOADER) {
    downloader = true;
}

if (downloader) {
    console.log("[SHOA] Iniciando en modo Downloader");
    require("./lib/Downloader").start();
} else {
    const proveedorCapas = new ProveedorCapasSHOA({
        puertoHTTP:config.webServer.http.port,
        directorioWeb:__dirname + "/www",
        directorioPublicacion:null
    });
    if (process.argv.length == 4) {
        if (process.argv[2] == "-cmd") {
            let a = process.argv[3];
            proveedorCapas[a]();
        }
    } else {
        proveedorCapas.start();
    }
}