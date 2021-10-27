const csv = require('csvtojson');
let categories;
const yelp = require('./categories.json');

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

const pipeline = (...fns) => fns.reduce((f, g) => (...args) => g(f(...args)));

(async () => {
  categories = await csv().fromFile('./category-map.csv')
  const locations = [
    /*
      name: 'Tryon Public House',
      categories: 'Bars, Gastropubs, American (Traditional)',
    },
    {
      name: 'Bronx Alehouse',
      categories: 'Pubs, American (Traditional)',
    },
    {
      name: 'Accent Reduction and ESL for Business',
      categories: 'Private Tutors, Adult Education, Language Schools',
    },
    {
      name: 'Columbia Ob/Gyn Uptown',
      categories: 'Medical Centers, Obstetricians & Gynecologists, Adult Education'
    },
    {
      name: 'Glam House Bx',
      categories: 'Party & Event Planning, Venues & Event Spaces, Party Equipment Rentals'
    },
    {
      name: 'Lincoln Technical Institute - Queens',
      categories: 'Colleges & Universities, Adult Education, Vocational & Technical School'
    },
    {
      name: 'Samuel Field Y',
      categories: 'Preschools, Community Service/Non-Profit, Child Care & Day Care'
    },
    {
      name: 'Bohemian Hall & Beer Garden',
      categories: 'Beer Gardens, Venues & Event Spaces',
    },
    {
      name: 'Hotel 64',
      categories: 'Hotels',
    },
    {
      name: 'Threes Brewing',
      categories: 'Beer Bar, Burgers, Venues & Event Spaces',
    },*/
    {
      name: 'Today S Learning Center',
      categories: '8351-00 Child day care services',
    },
    {
      name: 'RELISH CATERERS',
      categories: '',
    },
    {
      name: 'SHOKUNIN BBQ',
      categories: 'RESTAURANT',
    },
    {
      name: 'SHOKUNIN BBQ',
      categories: 'RESTAURANT',
    },
    {
      name: '',
      categories: '5499-0202 Juices, fruit or vegetable',
    },
    {
      name: 'il Miglio Brick Oven Pizzeria & Italian Restaurant',
      categories: 'Pizza, Italian',
    },
    {
      name: 'New York Botanical Garden',
      categories: 'Botanical Gardens',
    },
    {
      name: 'JP Morgan Chase Cafeteria',
      categories: 'Banks & Credit Unions, American (New)',
    },
    {
      name: 'South End Press',
      categories: 'Community Service/Non-Profit, Print Media',
    },
    {
      name: 'Evergreen Liquor Store Inc',
      categories: 'Beer, Wine & Spirits',
    },
  ]
  console.log();
  for (location of locations) {
    console.log(`Name:\t\t${location.name}`);
    const from = getCategories(location.categories);
    if (location.categories) {
      console.log(`Original:\t${from}`);
      const matched = matchCategories(from);
      console.log(`Matched:\t${matched}`);
      const newCats = renameCategories(matched);
      console.log(`New names:\t${newCats}`);
      const weights = getWeights(newCats);
      console.log(`Weights:`);
      console.log(weights);
      const heaviest = getHeaviest(weights);
      console.log(`Main Category:\t${heaviest}`);
      console.log();
    }

    /*
    console.log(`Name:\t\t${location.name}`);
    console.log(`Categories:\t${location.categories}`);
    const winner = pipeline(getCategories, matchCategories, renameCategories, getWeights, getHeaviest)(location.categories);
    console.log(`Main:\t\t${winner}\n`);
    */
  }
})();
