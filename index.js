const Influx = require('influx');
const DarkSky = require('dark-sky');
const Cron = require('node-cron');

const INFLUX_DB_HOST = process.env.INFLUX_DB_HOST || "influxdb.rydbir.local";
const DARKSKY_API_KEY = process.env.DARKSKY_API_KEY;
const CONFIG_FILE = process.env.CONFIG_FILE || './config.json';
const USE_CRON = true;

const CONFIG = require(CONFIG_FILE);

// Main

const forecastDb = new Influx.InfluxDB({
  host: INFLUX_DB_HOST,
  database: 'forecast',
  schema: [
    {
      measurement: 'hourly',
      fields: {
        latitude: Influx.FieldType.FLOAT,
        longitude: Influx.FieldType.FLOAT,
        summary: Influx.FieldType.STRING,
        temperature: Influx.FieldType.FLOAT,
        dewPoint: Influx.FieldType.FLOAT,
        humidity: Influx.FieldType.FLOAT,
        pressure: Influx.FieldType.FLOAT,
        windSpeed: Influx.FieldType.FLOAT,
        windBearing: Influx.FieldType.INTEGER,
        cloudCover: Influx.FieldType.FLOAT,
      },
      tags: [
        'icon', 'location'
      ]
    },
    {
      measurement: 'currently',
      fields: {
        latitude: Influx.FieldType.FLOAT,
        longitude: Influx.FieldType.FLOAT,
        summary: Influx.FieldType.STRING,
        temperature: Influx.FieldType.FLOAT,
        dewPoint: Influx.FieldType.FLOAT,
        humidity: Influx.FieldType.FLOAT,
        pressure: Influx.FieldType.FLOAT,
        windSpeed: Influx.FieldType.FLOAT,
        windBearing: Influx.FieldType.INTEGER,
        cloudCover: Influx.FieldType.FLOAT,
      },
      tags: [
        'icon', 'location'
      ]
    },
  ]
});

const darkskyApi = new DarkSky(DARKSKY_API_KEY);

setupDb().then(() => {
  if (USE_CRON) {
    Cron.schedule('*/5 * * * *', () => {
      collectForecastData()
    });
  } else {
    collectForecastData()
  }
})
.catch(console.error);

// Functions

function setupDb() {
  return new Promise((resolve, reject) => {
    forecastDb.getDatabaseNames().then(names => {
      if (!names.find(n => n === 'forecast')) {
        forecastDb.createDatabase('forecast').then(resolve).catch(reject);
      }
      resolve();
    }).catch(reject);
  });
}

function collectForecastData() {
  const darkskyRequests = [];

  CONFIG.locations.forEach(location => {
    const p = new Promise((resolve, reject) => {
      darkskyApi.latitude(location.latitude).longitude(location.longitude)
      .units('si')
      .exclude('daily,alerts')
      .get()
      .then(data => {
        const hourly = data.hourly;
        const c = data.currently;
        let points = [];
        hourly.data.forEach(d => {
          points.push({
            measurement: 'hourly',
            fields: {
              latitude: location.latitude,
              longitude: location.longitude,
              summary: d.summary,
              temperature: d.temperature,
              dewPoint: d.dewPoint,
              humidity: d.humidity,
              pressure: d.pressure,
              windSpeed: d.windSpeed,
              windBearing: d.windBearing,
              cloudCover: d.cloudCover,
            },
            tags: { icon: d.icon, location: location.name },
            timestamp: d.time + '000000000',
          });
        });
        points.push({
          measurement: 'currently',
          fields: {
            latitude: location.latitude,
            longitude: location.longitude,
            summary: c.summary,
            temperature: c.temperature,
            dewPoint: c.dewPoint,
            humidity: c.humidity,
            pressure: c.pressure,
            windSpeed: c.windSpeed,
            windBearing: c.windBearing,
            cloudCover: c.cloudCover,
          },
          tags: { icon: c.icon, location: location.name },
          timestamp: c.time + '000000000',
        });
        forecastDb.writePoints(points)
        .then(() => {
          console.log("Forecast data for '" + location.name + "' collected and sent to Influx DB");
          resolve();          
        })
        .catch(e => {
          reject(e.message);
        });
      })
      .catch(reject);
    });
    darkskyRequests.push(p);
  })
  Promise.all(darkskyRequests)
  .then(() => {
    console.log('Processed ' + CONFIG.locations.length + ' location(s)');
  })
  .catch(console.error);
}

