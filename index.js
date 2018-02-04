const Influx = require('influx');
const DarkSky = require('dark-sky');
const Cron = require('node-cron');

const INFLUX_DB_HOST = process.env.INFLUX_DB_HOST || "influxdb.rydbir.local";
const DARKSKY_API_KEY = process.env.DARKSKY_API_KEY;

const WEATHER_LOCATIONS = [
  {
    name: 'solna',
    latitude: 59.3775869,
    longitude: 18.010939,
  },
  {
    name: 'löa',
    latitude: 59.8075448,
    longitude: 15.1664593,
  },
  {
    name: 'umeå',
    latitude: 63.8258471,
    longitude: 20.2630354,
  }
];

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
      },
      tags: [
        'icon', 'location'
      ]
    },
  ]
});

const darkskyApi = new DarkSky(DARKSKY_API_KEY);

setupDb().then(() => {
  Cron.schedule('*/5 * * * *', () => {
    collectForecastData()
  });
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

  WEATHER_LOCATIONS.forEach(location => {
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
    console.log('Processed ' + WEATHER_LOCATIONS.length + ' location(s)');
  })
  .catch(console.error);
}

