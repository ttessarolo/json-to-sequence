"use strict";
import * as percom from "percom";
import * as clone from "clone";
import * as fs from "fs";
import * as serialize from "serialize-javascript";
import { gzipSync, unzipSync } from "zlib";

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

function shuffleArray(array, shuffle = true) {
  if (!shuffle) return array;

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

function getKeys(alphabet, n, shuffle = true) {
  const k = getLength(alphabet, n);
  const alphabets = alphabet.slice(0, k);
  const permutations = percom.per(alphabets, k);

  return shuffleArray(
    permutations.map((p) => p.join("")),
    shuffle
  );
}

function addToAlphabets(alphabet, seq) {
  seq.forEach((s) => {
    s.split("").forEach((k) => alphabet.add(k));
  });
}

function chunkString(str, length, tokenSeparator) {
  if (tokenSeparator) return str.split(tokenSeparator);
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
    model,
    path,
    name = `model_${Date.now()}`,
    fields = [],
    skipFields = [],
    fillNA = false,
    uniformTokenLength = true,
    NA = "X",
    keyAlphabet = alphabet,
    valuesAlphabet = numbers,
    verbose = false,

    filterData = () => false,
    translateKey = (k) => k,
    translateValue = (k, v) => v,
  }) {
    if (model) {
      this.extractDataFromModel(model);
      this.model = model;
    } else if (path) {
      this.loadModel(path);
    } else {
      this.fields = fields;
      this.skipFields = skipFields;
      this.fillNA = fillNA;
      this.uniformTokenLength = uniformTokenLength;
      this.NA = NA;
      this.keyAlphabet = keyAlphabet;
      this.valuesAlphabet = valuesAlphabet;
    }

    if (!this.name) this.name = name;
    if (!this.filterData) this.filterData = filterData;
    if (!this.translateKey) this.translateKey = translateKey;
    if (!this.translateValue) this.translateValue = translateValue;

    this.verbose = verbose;
    this.combinations = new Set();
  }

  getCombinations() {
    return this.combinations;
  }

  fit({ data, shuffleValueAlphabets = true }) {
    const _keys = new Map();
    const _values = new Set();

    const df = {
      keysSize: 0,
      valuesSize: 0,
      translators: {},
      inverters: {},
      alphabets: new Set(),
    };

    data = Array.isArray(data) ? data : [data];
    for (const row of data) {
      for (let key of this.fields) {
        if (!this.skipFields.includes(key)) {
          let value = row[key];
          if (value) {
            key = this.translateKey(key);
            if (!_keys.has(key)) _keys.set(key, new Set());
            df.keysSize += 1;

            const values = Array.isArray(value) ? value : [value];
            for (let valore of values) {
              valore = this.translateValue(key, valore);
              _values.add(valore);
              _keys.get(key).add(valore);
            }
          }
        }
      }
    }

    const keysAlpha = getKeys(this.keyAlphabet, _keys.size);
    addToAlphabets(df.alphabets, keysAlpha);

    let keyLength = keysAlpha[0].length;
    let valueLength = 0;
    let fixedValuesAlpha;

    if (this.uniformTokenLength) {
      fixedValuesAlpha = getKeys(this.valuesAlphabet, _values.size, shuffleValueAlphabets);
    }

    for (const [key, value] of _keys.entries()) {
      const valuesAlpha =
        fixedValuesAlpha || getKeys(this.valuesAlphabet, value.size, shuffleValueAlphabets);

      valueLength = valuesAlpha[0].length;
      addToAlphabets(df.alphabets, valuesAlpha);

      const valuesMap = new Map();
      const inverseValuesMap = new Map();
      for (const valore of [...value]) {
        if (this.filterData(key, valore)) continue;
        const val = valuesAlpha.splice(0, 1)[0];
        valuesMap.set(valore, val);
        inverseValuesMap.set(val, valore);
      }

      const chiave = keysAlpha.splice(0, 1)[0];
      df.translators[key] = { key: chiave, values: valuesMap };
      df.inverters[chiave] = { key, values: inverseValuesMap };
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

    df.valuesSize = _values.size;
    this.model = df;

    if (this.verbose) console.log(df);
    return df;
  }

  transform({ data, tokenSeparator = "" }) {
    const results = [];
    const errors = [];
    const multiple = Array.isArray(data);
    if (!multiple) data = [data];

    for (const row of data) {
      const d = [];
      for (let key of this.fields) {
        if (!this.skipFields.includes(key)) {
          const value = row[key];

          key = this.translateKey(key);
          const translate = this.model.translators[key];

          if (!translate) {
            errors.push(`Key ${key} is not in the Model. Maybe you should refit it.`);
            continue;
          }

          if (value) {
            const values = Array.isArray(value) ? value : [value];
            for (let valore of values) {
              if (this.filterData(key, valore)) continue;

              valore = this.translateValue(key, valore);
              const translated = translate.values.get(valore);
              if (translated) {
                const seq = `${translate.key}${translate.values.get(valore)}`;
                d.push(seq);
                this.combinations.add(seq);
              } else errors.push(`Value ${valore} is not in the Model. Maybe you should refit it.`);
            }
          } else if (this.fillNA) {
            if (this.uniformTokenLength)
              d.push(translate.key + this.NA.repeat(this.model.valueLength));
            else d.push(`${translate.key}${this.NA}`);
          }
        }
      }

      const seq = d.join(tokenSeparator);
      if (this.verbose) console.log(seq.length, seq);

      results.push(seq);
    }

    return [multiple ? results : results[0], errors];
  }

  invert({ data, tokenSeparator = "" }) {
    if (!this.model.uniformTokenLength && !tokenSeparator) {
      throw "The Model has not fixed token length. You Should specify a token separator.";
    }

    const errors = [];
    const results = [];
    const multiple = Array.isArray(data);
    if (!multiple) data = [data];

    const tokenLength = this.model.tokenLength;
    const keyLength = this.model.keyLength;

    if (tokenLength || tokenSeparator) {
      for (const row of data) {
        for (const token of chunkString(row, tokenLength, tokenSeparator)) {
          const [key, value] = getKeyValue(token, keyLength);
          const k = this.model.inverters[key];

          if (k) {
            const v = k.values.get(value);
            if (v) {
              results.push([k.key, k.values.get(value)]);
            } else errors.push(`Value ${v} is not in the Model. Maybe you should refit it.`);
          } else errors.push(`Key ${k} is not in the Model. Maybe you should refit it.`);
        }
      }
    } else errors.push("No Valid Model Loaded");

    return [multiple ? results : results[0], errors];
  }

  getAlphabets() {
    if (this.model && this.model.alphabets) {
      return this.model.alphabets;
    }
  }

  extractDataFromModel(model) {
    this.name = model.name;
    this.fields = model.fields;
    this.skipFields = model.skipFields;
    this.fillNA = model.fillNA;
    this.uniformTokenLength = model.uniformTokenLength;
    this.NA = model.NA;
    this.keyAlphabet = model.keyAlphabet;
    this.valuesAlphabet = model.valuesAlphabet;
    this.filterData = model.filterData;
    this.translateKey = model.translateKey;
    this.translateValue = model.translateValue;
  }

  saveModel({ dir, generateJSONCopy = false }) {
    if (this.model) {
      const model = clone.default(this.model);
      model.name = this.name;
      model.fields = this.fields;
      model.skipFields = this.skipFields;
      model.fillNA = this.fillNA;
      model.uniformTokenLength = this.uniformTokenLength;
      model.NA = this.NA;
      model.keyAlphabet = this.keyAlphabet;
      model.valuesAlphabet = this.valuesAlphabet;
      model.filterData = this.filterData;
      model.translateKey = this.translateKey;
      model.translateValue = this.translateValue;
      model.creationDate = Date.now();

      if (!dir.endsWith("/")) dir = `${dir}/`;
      const serialized = serialize.default(model);

      const compressed = gzipSync(Buffer.from(serialized));
      fs.writeFileSync(`${dir}${model.name}.j2s`, compressed);

      if (generateJSONCopy) {
        Object.keys(model.translators).forEach((key) => {
          model.translators[key].values = [...model.translators[key].values];
        });
        Object.keys(model.inverters).forEach((key) => {
          model.inverters[key].values = [...model.inverters[key].values];
        });
        fs.writeFileSync(`${dir}${model.name}.json`, JSON.stringify(model, null, 1));
      }
    } else throw new Error("No Model To Save");
  }

  loadModel(path) {
    const raw = fs.readFileSync(path);
    const decompressed = unzipSync(raw);
    const model = eval("(" + decompressed + ")");

    this.extractDataFromModel(model);

    if (this.verbose) console.log(model);

    this.model = model;
  }
}
