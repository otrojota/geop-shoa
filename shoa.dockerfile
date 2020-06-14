# docker build -f shoa.dockerfile -t otrojota/geoportal:shoa-0.16 .
# docker push otrojota/geoportal:shoa-0.16
#
FROM otrojota/geoportal:gdal-nodejs
WORKDIR /opt/geoportal/geop-shoa
COPY . .
RUN apt-get update
RUN apt-get -y install git
RUN npm install 
EXPOSE 8192
CMD node index.js