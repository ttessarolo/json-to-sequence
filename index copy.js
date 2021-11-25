"use strict";
import * as percom from "percom";
import * as clone from "clone";
import * as fs from "fs";

const alphabet = [
  "A",
  "B",
  "C",
  "D",
  "E",
  "F",
  "G",
  "H",
  "I",
  "J",
  "K",
  "L",
  "M",
  "N",
  "O",
  "P",
  "Q",
  "R",
  "S",
  "T",
  "U",
  "V",
  "W",
  "Y",
  "Z",
];

const numbers = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"];

function shuffleArray(array) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    const temp = array[i];
    array[i] = array[j];
    array[j] = temp;
  }

  return array;
}

function getLength(alphabet, n) {
  for (let k = 2; k < alphabet.length; k++) {
    const p = percom.countPer(k, k);
    if (p >= n) return k;
  }
}

function getKeys(alphabet, n) {
  const k = getLength(alphabet, n);
  const alphabets = alphabet.slice(0, k);
  const permutations = percom.per(alphabets, k);

  return shuffleArray(permutations.map((p) => p.join("")));
}

function addToAlphabets(alphabet, seq) {
  seq.forEach((s) => {
    s.split("").forEach((k) => alphabet.add(k));
  });
}

function chunkString(str, length) {
  return str.match(new RegExp(".{1," + length + "}", "g"));
}

function getKeyValue(str, keyLength) {
  const arr = str.split("");
  const key = arr.slice(0, keyLength).join("");
  const value = arr.slice(keyLength).join("");

  return [key, value];
}

export default class JSONSequencer {
  constructor({
    model = {},
    path,
    fields = [],
    skipFields = [],
    fillNA = false,
    uniformTokenLength = false,
    NA = "X",
    keyAlphabet = alphabet,
    valuesAlphabet = numbers,
    verbose = false,

    filterData = () => false,
    translateKey = (k) => k,
    translateValue = (v) => v,
  }) {
    this.model = model;
    this.fields = fields;
    this.skipFields = skipFields;
    this.fillNA = fillNA;
    this.uniformTokenLength = uniformTokenLength;
    this.NA = NA;
    this.keyAlphabet = keyAlphabet;
    this.valuesAlphabet = valuesAlphabet;

    this.verbose = verbose;
    this.filterData = filterData;
    this.translateKey = translateKey;
    this.translateValue = translateValue;

    this.combinations = new Set();

    if (path) this.loadModel(path);
  }

  getCombinations() {
    return this.combinations;
  }

  fit({ data }) {
    const df = {
      keys: new Map(),
      keysSize: 0,
      valuesSize: 0,
      translators: {},
      inverters: {},
      alphabets: new Set(),
    };

    data = Array.isArray(data) ? data : [data];
    for (const row of data) {
      for (let [key, value] of Object.entries(row)) {
        if (!this.skipFields.includes(key)) {
          key = this.translateKey(key);
          if (!df.keys.has(key)) df.keys.set(key, new Set());
          df.keysSize += 1;

          const values = Array.isArray(value) ? value : [value];
          for (let valore of values) {
            valore = this.translateValue(valore);
            df.keys.get(key).add(valore);
          }
        }
      }
    }

    const keysAlpha = getKeys(this.keyAlphabet, df.keys.size);
    addToAlphabets(df.alphabets, keysAlpha);

    let keyLength = keysAlpha[0].length;
    let valueLength = 0;
    let k = 0;
    let fixedValuesAlpha;

    if (this.uniformTokenLength) {
      let maxValueLength = 0;
      for (const value of df.keys.values()) {
        if (value.size > maxValueLength) maxValueLength = value.size;
      }
      fixedValuesAlpha = getKeys(this.valuesAlphabet, maxValueLength);
    }

    for (const [key, value] of df.keys.entries()) {
      const valuesAlpha = fixedValuesAlpha ? shuffleArray(fixedValuesAlpha) : getKeys(value.size);

      valueLength = valuesAlpha[0].length;
      addToAlphabets(df.alphabets, valuesAlpha);

      let v = 0;
      const valuesMap = new Map();
      const inverseValuesMap = new Map();
      for (const valore of [...value]) {
        if (this.filterData(key, valore)) continue;

        df.valuesSize += 1;
        valuesMap.set(valore, valuesAlpha[v]);
        inverseValuesMap.set(valuesAlpha[v], valore);
        v = v + 1;
      }

      df.translators[key] = { key: keysAlpha[k], values: valuesMap };
      df.inverters[keysAlpha[k]] = { key, values: inverseValuesMap };
      k = k + 1;
    }

    df.alphabets = [...df.alphabets];

    if (this.fillNA) {
      df.alphabets.unshift(this.NA);
    }

    df.alphabets = df.alphabets.sort();

    if (this.uniformTokenLength) {
      df.keyLength = keyLength;
      df.valueLength = valueLength;
      df.tokenLength = keyLength + valueLength;
    }

    delete df.keys;

    this.model = df;

    if (this.verbose) console.log(df);
    return df;
  }

  transform(dataset) {
    const results = [];
    const multiple = Array.isArray(dataset);
    if (!multiple) dataset = [dataset];

    for (const data of dataset) {
      const d = [];
      for (let key of this.fields) {
        if (!this.skipFields.includes(key)) {
          const translate = this.model.translators[key];

          if (!translate) {
            throw new Error(`Key ${key} is not in the Model. Maybe you should refit it.`);
          }

          const value = data[key];

          if (value) {
            const values = Array.isArray(value) ? value : [value];
            for (const valore of values) {
              if (this.filterData(key, valore)) continue;

              const translated = translate.values.get(valore);
              if (translated) {
                const seq = `${translate.key}${translate.values.get(valore)}`;
                d.push(seq);
                this.combinations.add(seq);
              } else
                throw new Error(`Value ${valore} is not in the Model. Maybe you should refit it.`);
            }
          } else if (this.fillNA) {
            if (this.uniformTokenLength)
              d.push(translate.key + this.NA.repeat(this.model.valueLength));
            else d.push(`${translate.key}${this.NA}`);
          }
        }
      }

      const seq = d.join("");
      if (this.verbose) console.log(seq.length, seq);

      results.push(seq);
    }

    return multiple ? results : results[0];
  }

  invert(dataset) {
    if (!this.model.uniformTokenLength) {
      throw new Error("Invert is possible only on uniformTokenLength");
    }

    const results = [];
    const multiple = Array.isArray(dataset);
    if (!multiple) dataset = [dataset];

    const tokenLength = this.model.tokenLength;
    const keyLength = this.model.keyLength;

    if (keyLength && tokenLength) {
      for (const data of dataset) {
        for (const token of chunkString(data, tokenLength)) {
          const [key, value] = getKeyValue(token, keyLength);
          const k = this.model.inverters[key];

          if (k) {
            const v = k.values.get(value);
            if (v) {
              results.push([k.key, k.values.get(value)]);
            } else throw new Error(`Value ${v} is not in the Model. Maybe you should refit it.`);
          } else throw new Error(`Key ${k} is not in the Model. Maybe you should refit it.`);
        }
      }
    } else throw new Error("No Valid Model Loaded");

    return multiple ? results : results[0];
  }

  getAlphabets() {
    if (this.model && this.model.alphabets) {
      return this.model.alphabets;
    }
  }

  saveModel(path) {
    if (this.model) {
      const model = clone.default(this.model);
      model.fields = this.fields;
      model.skipFields = this.skipFields;
      model.fillNA = this.fillNA;
      model.uniformTokenLength = this.uniformTokenLength;
      model.NA = this.NA;
      model.keyAlphabet = this.keyAlphabet;
      model.valuesAlphabet = this.valuesAlphabet;
      model.creationDate = Date.now();

      Object.keys(model.translators).forEach((key) => {
        model.translators[key].values = [...model.translators[key].values];
      });

      Object.keys(model.inverters).forEach((key) => {
        model.inverters[key].values = [...model.inverters[key].values];
      });

      fs.writeFileSync(path, JSON.stringify(model, null, 1));
    } else throw new Error("No Model To Save");
  }

  loadModel(path) {
    const raw = fs.readFileSync(path);
    const model = JSON.parse(raw);

    Object.keys(model.translators).forEach((key) => {
      model.translators[key].values = new Map(model.translators[key].values);
    });

    Object.keys(model.inverters).forEach((key) => {
      model.inverters[key].values = new Map(model.inverters[key].values);
    });

    this.fields = model.fields;
    this.skipFields = model.skipFields;
    this.fillNA = model.fillNA;
    this.uniformTokenLength = model.uniformTokenLength;
    this.NA = model.NA;
    this.keyAlphabet = model.keyAlphabet;
    this.valuesAlphabet = model.valuesAlphabet;

    if (this.verbose) console.log(model);

    this.model = model;
  }
}
