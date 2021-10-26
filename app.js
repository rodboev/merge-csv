const csv = require('csvtojson');
const mongoose = require('mongoose');
const schema = require('./schema.json');

const area = "nyc";

const zips = require(`./zips-${area}.json`);
const zipcodes = zips.map(obj => String(obj["Zip Code"]));

const businessSchema = new mongoose.Schema(schema);
const Business = mongoose.model('Business', businessSchema);

mongoose.connect(`mongodb://localhost:27017/filtered-${area}`);

async function insertPB() {
  // PhoneBurner
  const phoneburnerCsv = `phoneburner-${area}.csv`;
  console.log(`Inserting PhoneBurner data from ${phoneburnerCsv}...`);
  const phoneburnerJson = await csv().fromFile(phoneburnerCsv)
  const phoneburnerFields = ["Address1", "Address2", "Industry", "City", "Company Name", "Business Category", "Email", "First Name", "Last Name", "Latitude", "Longitude", "Notes", "Phone", "Phone Type", "State", "Tags", "Zip", "City"]
  
  for (entry of phoneburnerJson) {
    if (entry["Phone"] && entry["Phone"] !== "null" && zipcodes.includes(entry["Zip"])) {
      const newObj = {};
      for (field of phoneburnerFields) {
        if (entry[field] && entry[field] !== "null") {
          newObj[field] = entry[field];
        }
      }
      newObj["County"] = zips.find(obj => obj["Zip Code"] == entry["Zip"])?.County;
      const filter = { "Phone": newObj["Phone"] };
      await Business.findOneAndUpdate(filter, newObj, {
        upsert: true // insert if not found
      })
    }
  }
  console.log(`Done with ${phoneburnerCsv}`);
}

async function insertYelp(yelpCsv) {
  /* Yelp fields: 
  name
  phone
  categories
  address1
  address2
  address3
  city
  state
  zip
  latitude
  longitude
  */
  console.log(`Inserting Yelp data from ${yelpCsv}...`);
  const yelpJson = await csv().fromFile(yelpCsv);

  for (entry of yelpJson) {
    if (zipcodes.includes(entry.zip)) {
      const newObj = {
        "Phone": entry.phone,
        "Company Name": entry.name,
        "Yelp Categories": entry.categories,
        "Address1": entry.address1,
        "Address2": [entry.address2, entry.address3].filter(str => Boolean(str) && str !== "null").join(", "),
        "City": entry.city,
        "State": entry.state,
        "Zip": entry.zip,
        "Latitude": entry.latitude,
        "Longitude": entry.longitude,
        "County": zips.find(obj => obj["Zip Code"] == entry.zip)?.County,
      }
      for (prop in newObj) {
        if (newObj[prop] === "null") delete newObj[prop];
      }
      const filter = { "Phone": newObj["Phone"] };
      await Business.findOneAndUpdate(filter, newObj, {
        upsert: true // insert if not found
      })
    }
  }

  console.log(`Done with ${yelpCsv}`);
}

(async() => {
  await insertPB();
  await insertYelp(`businesses-${area}-filtered-clean.csv`);
  // await insertYelp("businesses-nassau-clean.csv");
  process.exit(1)
})();