const csv = require('csvtojson');
const mongoose = require('mongoose');
const schema = require('./schema.json');

const area = "nyc";

let zips, zipcodes;

const businessSchema = new mongoose.Schema(schema);
const Business = mongoose.model('Business', businessSchema);

mongoose.connect(`mongodb://localhost:27017/filtered-${area}`);

const yelp = require('./categories.json');
let categories;
const pipeline = (...fns) => fns.reduce((f, g) => (...args) => g(f(...args)));
const findMainCat = pipeline(getCategories, matchCategories, renameCategories, getWeights, getHeaviest);

async function insertPB() {
  // PhoneBurner
  const phoneburnerCsv = `phoneburner-${area}.csv`;
  console.log(`Inserting PhoneBurner data from ${phoneburnerCsv}...`);
  const phoneburnerJson = await csv().fromFile(phoneburnerCsv)
  const phoneburnerFields = ["Address1", "Address2", "City", "Company Name", "Business Category", "Email", "First Name", "Last Name", "Latitude", "Longitude", "Notes", "Phone", "Phone Type", "State", "Tags", "Zip", "City"]
  categories = await csv().fromFile('./category-map.csv')
  
  for (entry of phoneburnerJson) {
    entry["Zip"] = entry["Zip"]?.padStart(5, "0").substr(0,5);
    if (entry["Phone"] && entry["Phone"] !== "null" && zipcodes.includes(entry["Zip"])) {
      const newObj = {};
      
      for (field of phoneburnerFields) {
        if (entry[field] && entry[field] !== "null") {
          newObj[field] = entry[field];
        }
      }
      newObj["County"] = zips.find(obj => obj["Zip Code"] == entry["Zip"])?.County;
      if (entry["Business Category"]) {
        // console.log(`Working on ${entry["Company Name"]} ${entry["Phone"]} (${entry["Business Category"]})`)
        newObj["Main Category"] = findMainCat(entry["Business Category"]);
      }
      
      const filter = { "Phone": newObj["Phone"] };
      await Business.findOneAndUpdate(filter, newObj, {
        upsert: true // insert if not found
      })
    }
    else {
      if (entry["Phone"]?.length > 0) {
        // console.log(`Skipped ${entry["Phone"] || "[no phone]"} in ${entry["Zip"]}`);
      }
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
  categories = await csv().fromFile('./category-map.csv')

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
      if (entry.categories) {
        // console.log(`Working on ${entry.name} ${entry.phone} (${entry.categories})`)
        newObj["Main Category"] = findMainCat(entry.categories);
      }
      for (prop in newObj) {
        if (newObj[prop] === "null") delete newObj[prop];
      }
      const filter = { "Phone": newObj["Phone"] };
      await Business.findOneAndUpdate(filter, newObj, {
        upsert: true // insert if not found
      })
    }
    else {
      // console.log(`Skipped ${entry.phone || "[no phone]"} in ${entry.zip}`);
    }
  }

  console.log(`Done with ${yelpCsv}`);
}

function findParentByAlias(alias) {
  const cat = yelp.find(o => o.alias === alias);
  const parent = cat.parents;
  const title = cat.title;
	
  if (parent.length === 0) return alias;
  if (categories.find(o => o.category === title)) return alias;

  // console.log(`${cat.alias} --> ${parent} (parent)`);
  // console.log(`${cat.title} --> ${parent} (parent)`);
  return findParentByAlias(parent[0]);
}

function findName(alias) {
  return yelp.find(o => o.alias === alias).title;
}

function matchCategory(cat) {
  const categoryInList = categories.find(o => o.category.toLowerCase().trim() === cat.toLowerCase().trim());
  if (categoryInList) {
    // console.log(`${cat} --> OK`);
    return categoryInList.category;
  }
  else {
    const categoryInYelp = yelp.find(o => o.title.toLowerCase().trim() === cat.toLowerCase().trim());
    if (categoryInYelp) {
      const alias = categoryInYelp.alias;
      const parentAlias = findParentByAlias(alias);
      const parent = findName(parentAlias);
      // console.log(`${cat} --> ${parentAlias} (parent) --> ${parent}`);
      if (categories.find(o => o.category === parent)) {
        return parent;
      }
      else {
        matchCategory(parent);
      }
    }
  }
}

function matchCategories(cats) {
  let matched = [];
  for (cat of cats) {
    matched.push(matchCategory(cat));
  }
  return matched;
}

function renameCategory(cat) {
  const categoryInList = categories.find(o => o.category === cat);
  if (categoryInList) {
    return categories.find(o => o.category === cat).newCategory;
  }
}

function renameCategories(cats) {
  let renamed = [];
  for (cat of cats) {
    renamed.push(renameCategory(cat));
    // console.log(`Adding ${renameCategory(cat)} to category list`);
  }
  return renamed;
}

function getWeights(cats) {
  let weights = [];
  for (cat of cats) {
    const weight = categories.map(o => o.newCategory).includes(cat) ?
      Number(categories.find(o => o.newCategory === cat).weight) :
      1;

    const categoryExists = weights.find(o => o.category === cat);
    if (categoryExists && categoryExists.weight) {
      categoryExists.weight += weight;
    }
    else {
      weights.push({
        category: cat,
        weight: weight
      });
    }
  }
  return weights;
}

function getHeaviest(weights) {
  const sorted = weights.sort(function(a, b) { return b.weight-a.weight });
  const max = sorted[0].weight;
  const atMax = sorted.filter(o => o.weight === max);
  const last = atMax[atMax.length-1];
  const first = atMax[0];
  if (atMax.length > 1) {
    const cats = atMax.map(o => o.category);
    // console.log(`[WARNING] Equal weights (${max}): ${cats.join(', ')}`);
    // console.log(`[WARNING] Using ${first.category}`);
  }
  return first.category;
}

function getCategories(str) {
  if (yelp.find(o => o.title === str) || categories.find(o => o.category === str)) return [str];

  if (str.includes(',') /*&& !str.match(/^\d/)*/) {
    const cats = str.split(',').map(s => s.trim());
    return cats;
  }
  else {
    return [str];
  }
}

(async() => {
  zips = await csv().fromFile(`zips-${area}.csv`)
  zipcodes = zips.map(obj => String(obj["Zip Code"]));
  await insertPB();
  await insertYelp(`businesses-${area}-filtered-clean.csv`);
  process.exit(1)
})();
