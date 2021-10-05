const csv = require('csvtojson');
const mongoose = require('mongoose');

const schema = require('./schema.json');

const businessSchema = new mongoose.Schema(schema);
const Business = mongoose.model('Business', businessSchema);

mongoose.connect('mongodb://localhost:27017/merged');

async function insertPB() {
  // PhoneBurner
  console.log("Inserting PhoneBurner data...");
  const phoneburnerCsv = "phoneburner.csv";
  const phoneburnerJson = await csv().fromFile(phoneburnerCsv)
  const phoneburnerFields = ["First Name", "Last Name", "Email", "Phone", "Phone Label", "Phone Type", "Phone 2", "Phone 2 Label", "Phone 2 Type", "Phone 3", "Phone 3 Label", "Phone 3 Type", "Phone 4", "Phone 4 Label", "Phone 4 Type", "Phone 5", "Phone 5 Label", "Phone 5 Type", "Address1", "Address2", "City", "State", "Zip", "Notes", "Tags", "Company Name", "Business Category"];

  for (entry of phoneburnerJson) {
    const newObj = {};
    for (field of phoneburnerFields) {
      if (entry[field] && entry[field] !== "null") {
        newObj[field] = entry[field];
      }
    }
    const filter = { "Phone": newObj["Phone"] };
    await Business.findOneAndUpdate(filter, newObj, {
      upsert: true // insert if not found
    })
  }
  /* await Business.insertMany(phoneburnerJson, {/
    ordered: false, // continue writing, even if a single write fails
    continueOnError: true
  }) */
  console.log("Done with PhoneBurner...");
}

async function insertYelp() {
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
  console.log("Inserting Yelp data...");
  const yelpCsv = "businesses-clean.csv";
  const yelpJson = await csv().fromFile(yelpCsv);

  for (entry of yelpJson) {
    const newObj = {
      "Phone": entry.phone,
      "Company Name": entry.name,
      "Business Categories": entry.categories,
      "Address1": entry.address1,
      "Address2": [entry.address2, entry.address3].filter(str => Boolean(str) && str !== "null").join(", "),
      "City": entry.city,
      "State": entry.state,
      "Zip": entry.zip,
      "Latitude": entry.latitude,
      "Longitude": entry.longitude,
    }
    const filter = { "Phone": newObj["Phone"] };
    await Business.findOneAndUpdate(filter, newObj, {
      upsert: true // insert if not found
    })
  }

  console.log("Done with Yelp...");
}

(async() => {
  await insertPB();
  await insertYelp();
})();