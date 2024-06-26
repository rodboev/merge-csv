const csv = require('csvtojson');
const mongoose = require('mongoose');
const schema = require('./schema.json');
const {compareTwoStrings: compare} = require('string-similarity');

const areas = [ "nyc", "upstate", "nassau", "nj" ]
const insertPhoneburner = false

let zips, zipcodes;

const businessSchema = new mongoose.Schema(schema);
const Business = mongoose.model('Business', businessSchema);

const yelp = require('./categories.json');
const yelpTitles = yelp.map(o => o.title);
const shortened = yelpTitles.reduce((a, title) => {
  if (title.endsWith('ies')) {
    const short = title.replace('ies', 'y').toLowerCase()
    a.push(short);
  }
  else if (title.endsWith('ers')) {
    const short = title.replace('ers', '').toLowerCase();
    if (!short.endsWith('t') && !short.endsWith('g') && !short.endsWith('b') && !short.endsWith('v') && !short.endsWith('i') && !short.endsWith('l')) {
      a.push(short);
    }
  }
  else if (title.endsWith('s')) {
    const short = title.replace(/s$/, '').toLowerCase()
    a.push(short)
  }
  return a;
}, []);

let categories;
const pipeline = (...fns) => fns.reduce((f, g) => (...args) => g(f(...args)));
const findMainCat = pipeline(getCategories, matchCategories, renameCategories, getWeights, getHeaviest);

async function insertPB({phoneburnerJson, yelpJson}) {
  // PhoneBurner
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
      
      if (!entry["Business Category"]) {
        const yelpListing = yelpJson.find(o =>
          o.address1.toLowerCase() === entry["Address1"].toLowerCase() ||
          o.name.toLowerCase().includes(entry["Company Name"].toLowerCase()) ||
          entry["Company Name"].toLowerCase().includes(o.name.toLowerCase())
          );

        if (yelpListing && yelpListing.categories) {
          newObj["Yelp Categories"] = yelpListing.categories;
          newObj["Main Category"] = findMainCat(newObj["Yelp Categories"]);
          // console.log(`${entry["Company Name"]} ${entry["Phone"]} assigned Yelp: ${newObj["Yelp Categories"]}, Main: ${newObj["Main Category"]} (cross-match)`);
        }
      }

      if (newObj["Business Category"] /*&& categories.some(o => o.category === newObj["Business Category"])*/) {
        newObj["Main Category"] = findMainCat(newObj["Business Category"]);
        // console.log(`${entry["Company Name"]} ${entry["Phone"]} Main: ${newObj["Main Category"]} (from Business: {newObj["Business Category"]})`);
      }

      if (newObj["Yelp Categories"]) {
        newObj["Main Category"] = findMainCat(newObj["Yelp Categories"]);
        // console.log(`${entry["Company Name"]} ${entry["Phone"]} Main: ${newObj["Main Category"]} (from Yelp: {newObj["Yelp Categories"]})`);
      }

      if (!newObj["Yelp Categories"] && !newObj["Main Category"]) {
        if (shortened.some(s => entry["Company Name"].toLowerCase().includes(s))) {
          newObj["Yelp Categories"] = yelpTitles.find(title => entry["Company Name"].toLowerCase().includes(title.replace(/ies$|ers$|s$/, '').toLowerCase()));
          newObj["Main Category"] = findMainCat(newObj["Yelp Categories"]);
          // console.log(`${entry["Company Name"]} ${entry["Phone"]} assigned Yelp: ${newObj["Yelp Categories"]}, Main: ${newObj["Main Category"]} (from Name)`);
        }
      }

      if (newObj["Business Category"] && !newObj["Yelp Categories"] && !newObj["Main Category"]) {
        if (yelpTitles.some(s => newObj["Business Category"].toLowerCase().includes(s.toLowerCase()))) {
          newObj["Yelp Categories"] = yelpTitles.filter(title => newObj["Business Category"].toLowerCase().includes(title.toLowerCase())).join("|");
          newObj["Main Category"] = findMainCat(newObj["Yelp Categories"]);
          // console.log(`${entry["Company Name"]} ${entry["Phone"]} assigned Yelp: ${newObj["Yelp Categories"]}, Main: ${newObj["Main Category"]} (from Business: ${newObj["Business Category"]})`);
        }
      }

      // Fuzzy match
      if (!newObj["Yelp Categories"] && !newObj["Main Category"]) {
        yelpListing = yelpJson.find(o =>
          (yelpStr = [o.name, o.address1, o.city, o.zip].join().toLowerCase()) &&
          (pbStr = [entry["Company Name"], entry["Address1"], entry["City"], entry["Zip"]].join().toLowerCase()) &&
          ((score = compare(yelpStr, pbStr)) > 0.625)
          );

        if (yelpListing) {
          if (score > 0.75) {
            if (!newObj["Yelp Categories"]) {
              newObj["Yelp Categories"] = yelpListing.categories;
            }
            newObj["Main Category"] = findMainCat(newObj["Yelp Categories"]);
            console.log(`${entry["Company Name"]} ${entry["Phone"]} assigned Yelp: ${newObj["Yelp Categories"]}, Main: ${newObj["Main Category"]} (${score})`);
          }
          else {
            newObj["Main Category"] = findMainCat(yelpListing.categories);
            console.log(`${entry["Company Name"]} ${entry["Phone"]} assigned Main: ${newObj["Main Category"]} (${score})`);
          }
        }
      }

      if (!newObj["Main Category"] && newObj["Yelp Categories"]) {
        newObj["Main Category"] = findMainCat(newObj["Yelp Categories"]);
        console.log(`${entry["Company Name"]} ${entry["Phone"]} assigned Main: ${newObj["Main Category"]} (fallback, from Yelp: ${newObj["Yelp Categories"]})`);
      }

      newObj["Yelp Categories"] && (newObj["Yelp Categories"] = newObj["Yelp Categories"].split('|').join(', '));
      
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
}

async function insertYelp({yelpJson}) {
  categories = await csv().fromFile('./category-map.csv')

  for (entry of yelpJson) {
    if (zipcodes.includes(entry.zip)) {
      const newObj = {
        "Phone": entry.phone,
        "Company Name": entry.name,
        "Yelp Categories": entry.categories.replace(/\|/g, ', '),
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
      const doc = await Business.findOneAndUpdate(filter, newObj, {
        upsert: true // insert if not found
      })
    }
    else {
      console.log(`Skipped ${entry.phone || "[no phone]"} in ${entry.zip}`);
    }
  }
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

  if (str.includes('|') /*&& !str.match(/^\d/)*/) {
    const cats = str.split('|').map(s => s.trim());
    return cats;
  }
  else {
    return [str];
  }
}

(async() => {
  zips = await csv().fromFile(`zips.csv`)
  zipcodes = zips.map(obj => String(obj["Zip Code"]));

  for (const area of areas) {
    await mongoose.disconnect()
    await mongoose.connect(`mongodb://localhost:27017/preflight-${area}`)
    const yelpCsv = `businesses-${area}-filtered-clean.csv`;
    const yelpJson = await csv().fromFile(yelpCsv);
    
    if (insertPhoneburner) {
      const phoneburnerCsv = `phoneburner-${area}.csv`;
      const phoneburnerJson = await csv().fromFile(phoneburnerCsv);
      console.log(`Inserting PhoneBurner data from ${phoneburnerCsv}...`);
      await insertPB({ phoneburnerJson, yelpJson });
      console.log(`Done with ${phoneburnerCsv}`);
    }

    console.log(`Inserting Yelp data from ${yelpCsv}...`);
    await insertYelp({ yelpJson });
    console.log(`Done with ${yelpCsv}`);
  }

  process.exit(1)
})();