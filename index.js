"use strict";
import * as percom from "percom";
import * as clone from "clone";
import * as fs from "fs";
import * as serialize from "serialize-javascript";
import get from "lodash.get";
import { gzipSync, unzipSync } from "zlib";
import Events from "events";

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

function checkStream(obj) {
  return obj != null && typeof obj.pipe === "function";
}

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

function getKeys(alphabet, n, shuffle = false) {
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

export default class JSONSequencer extends Events {
  constructor({
    model,
    path,
    name = `model_${Date.now()}`,
    fields = [],
    skipFields = [],
    autoupdate = false,
    autoupdateFactor = 1,
    fillNA = false,
    NA = "X",
    uniformTokenLength = false,
    tokenSeparator = " ",
    keyAlphabet = alphabet,
    valuesAlphabet = numbers,
    verbose = false,

    filterData = () => false,
    translateKey = (k) => k,
    translateValue = (k, v) => v,
  }) {
    super();

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
      this.tokenSeparator = tokenSeparator;
    }

    if (!this.name) this.name = name;
    if (!this.filterData) this.filterData = filterData;
    if (!this.translateKey) this.translateKey = translateKey;
    if (!this.translateValue) this.translateValue = translateValue;

    this.autoupdateFactor = this.autoupdateFactor ?? autoupdateFactor;
    this.autoupdate = this.autoupdate ?? autoupdate;
    this.verbose = verbose;
    this.combinations = new Set();

    if (this.autoupdate && this.autoupdateFactor <= 0) this.autoupdateFactor = 1;
  }

  getCombinations() {
    return this.combinations;
  }

  setParams({ skipFields, filterData, translateKey, translateValue }) {
    if (skipFields) this.skipFields = skipFields;
    if (filterData) this.filterData = filterData;
    if (translateKey) this.translateKey = translateKey;
    if (translateValue) this.translateValue;
  }

  async fit({ data, dataPath, shuffleValueAlphabets = false }) {
    const _keys = new Map();
    const _values = new Set();

    const df = {
      keysSize: 0,
      valuesSize: 0,
      translators: {},
      inverters: {},
      alphabets: new Set(),
    };

    data = checkStream(data) || Array.isArray(data) ? data : [data];
    for await (let row of data) {
      if (dataPath) row = get(row, dataPath);

      const campi = this.fields?.length > 0 ? this.fields : Object.keys(row);

      for (let key of campi) {
        if (!this.skipFields.includes(key)) {
          let value = row[key];
          if (value) {
            key = this.translateKey(key);
            if (!_keys.has(key)) _keys.set(key, new Set());

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

    df.keysSize += _keys.size;
    const keysAlpha = getKeys(
      this.keyAlphabet,
      _keys.size + (this.autoupdate ? this.autoupdateFactor : 0),
      shuffleValueAlphabets
    );
    addToAlphabets(df.alphabets, keysAlpha);

    let keyLength = keysAlpha[0].length;
    let valueLength = 0;
    let alfabetoValori = {};
    let maxAlphaValueSize = 0;

    for (const [key, value] of _keys.entries()) {
      const alphaValueSize =
        (this.uniformTokenLength ? _values.size : value.size) +
        (this.autoupdate ? this.autoupdateFactor : 0);

      if (alphaValueSize > maxAlphaValueSize) maxAlphaValueSize = alphaValueSize;
      const valuesAlpha = getKeys(this.valuesAlphabet, alphaValueSize, shuffleValueAlphabets);
      valueLength = valuesAlpha[0].length;
      addToAlphabets(df.alphabets, valuesAlpha);

      const valuesMap = new Map();
      const inverseValuesMap = new Map();
      for (const valore of [...value]) {
        if (this.filterData(key, valore)) continue;
        const valueKey = valuesAlpha.splice(0, 1)[0];
        valuesMap.set(valore, valueKey);
        inverseValuesMap.set(valueKey, valore);
      }

      alfabetoValori[key] = valuesAlpha;

      if (this.fillNA) {
        const missing = "Missing Value From Source";
        const valueKey = this.uniformTokenLength ? this.NA.repeat(valueLength) : this.NA;
        valuesMap.set(missing, valueKey);
        inverseValuesMap.set(valueKey, missing);
      }

      const chiave = keysAlpha.splice(0, 1)[0];
      df.translators[key] = { key: chiave, values: valuesMap };
      df.inverters[chiave] = { key, values: inverseValuesMap };
    }

    df.alphabets = [...df.alphabets];

    if (this.fillNA) {
      df.alphabets.unshift(this.NA);
    }
    if (this.tokenSeparator) {
      df.alphabets.unshift(this.tokenSeparator);
    }

    df.alphabets = df.alphabets.sort();

    if (this.uniformTokenLength) {
      df.keyLength = keyLength;
      df.valueLength = valueLength;
      df.tokenLength = keyLength + valueLength;
    }

    df.updater = { keys: keysAlpha, values: alfabetoValori };

    df.valuesSize = _values.size;
    df.maxAlphaValueSize = maxAlphaValueSize;
    this.model = df;

    if (this.verbose) console.log(df);
    this.emit("fitted");
  }

  _update(key, value, shuffleValueAlphabets) {
    try {
      // Missing Key
      if (!value) {
        const chiave = this.model.updater.keys.splice(0, 1)[0];
        if (chiave) {
          this.model.translators[key] = { key: chiave, values: new Map() };
          this.model.inverters[chiave] = { key: key, values: new Map() };
          this.model.updater.values[key] = getKeys(
            this.valuesAlphabet,
            this.model.maxAlphaValueSize,
            shuffleValueAlphabets
          );
          this.model.keysSize += 1;

          return this.model.translators[key];
        }
      }

      const valore = this.model.updater.values[key].splice(0, 1)[0];

      if (valore) {
        const { key: chiave, values } = this.model.translators[key];
        values.set(valore, key);
        this.model.inverters[chiave].values.set(key, valore);
        this.model.valuesSize += 1;
        return valore;
      }
    } catch (error) {}
  }

  async transform({ data, dataPath, outputStream, cb, shuffleValueAlphabets = false }) {
    if (outputStream && !checkStream(outputStream)) {
      throw new Error("outputStream must be a writable stream");
    }

    if (cb && !typeof cb === "function") {
      throw new Error("Callback must be a function");
    }

    let modelUpdated = false;
    const results = [];
    const errors = [];
    const isStream = checkStream(data);
    const multiple = Array.isArray(data);
    if (!multiple && !isStream) data = [data];

    for await (let chunk of data) {
      const row = dataPath ? get(chunk, dataPath) : chunk;
      const campi = this.fields?.length > 0 ? this.fields : Object.keys(row);
      const d = [];

      for (let key of campi) {
        if (!this.skipFields.includes(key)) {
          const value = row[key];

          key = this.translateKey(key);
          let translate = this.model.translators[key];

          if (!translate) {
            if (this.autoupdate) {
              translate = this._update(key, null, shuffleValueAlphabets);
              if (!translate) {
                errors.push(
                  `Model Autoupdate is out of capacity. Not able to update for key ${key} `
                );
                continue;
              } else modelUpdated = true;
            } else {
              errors.push(`Key ${key} is not in the Model. Maybe you should refit it.`);
              continue;
            }
          }

          if (value) {
            const values = Array.isArray(value) ? value : [value];
            for (let valore of values) {
              if (this.filterData(key, valore)) continue;

              valore = this.translateValue(key, valore);
              let translated = translate.values.get(valore);

              if (!translated) {
                if (this.autoupdate) {
                  translated = this._update(key, valore, shuffleValueAlphabets);
                  if (translated) modelUpdated = true;
                }
              }

              if (translated) {
                const seq = `${translate.key}${translated}`;
                d.push(seq);
                this.combinations.add(seq);
              } else
                errors.push(
                  this.autoupdate
                    ? `Model Autoupdate is out of capacity. Not able to update  ${valore} for key ${key}`
                    : `Value ${valore} for key ${key} is not in the Model. Maybe you should refit it.`
                );
            }
          } else if (this.fillNA) {
            if (this.uniformTokenLength)
              d.push(translate.key + this.NA.repeat(this.model.valueLength));
            else d.push(`${translate.key}${this.NA}`);
          }
        }
      }

      const seq = d.join(this.tokenSeparator);

      if (this.verbose) console.log(seq.length, seq);

      this.emit("transform_data", { sequence: seq, chunk });

      if (outputStream) outputStream.write({ sequence: seq, chunk });
      if (cb) cb(errors, seq, chunk);
      if (!outputStream && !cb) results.push(seq);
    }

    this.emit("transformed", results, errors);
    if (modelUpdated) this.emit("model_update", this.getModel());

    return [multiple ? results : results[0], errors];
  }

  fit_transform({
    data,
    transformData,
    dataPath,
    outputStream,
    cb,
    shuffleValueAlphabets = false,
  }) {
    if (isStream(data) && !transformData)
      throw new Error("Fit data is a stream you must provide transformData");

    this.fit({ data, shuffleValueAlphabets, dataPath });
    return this.transform({
      data: transformData ?? data,
      outputStream,
      cb,
      dataPath,
      shuffleValueAlphabets,
    });
  }

  async invert({ data, dataPath, outputStream, cb }) {
    if (!this.model.uniformTokenLength && !tokenSeparator) {
      throw "The Model has not fixed token length. You Should specify a token separator.";
    }

    if (outputStream && !checkStream(outputStream)) {
      throw new Error("outputStream must be a writable stream");
    }

    if (cb && !typeof cb === "function") {
      throw new Error("Callback must be a function");
    }

    const errors = [];
    const results = [];
    const isStream = checkStream(data);
    const multiple = Array.isArray(data);
    if (!multiple && !isStream) data = [data];

    const tokenLength = this.model.tokenLength;
    const keyLength = this.model.keyLength;

    if (tokenLength || this.tokenSeparator) {
      for await (let row of data) {
        if (dataPath) row = get(row, dataPath);

        for (const token of chunkString(row, tokenLength, this.tokenSeparator)) {
          const [key, value] = getKeyValue(token, keyLength);
          const k = this.model.inverters[key];

          if (k) {
            const v = k.values.get(value);
            if (v) {
              const ret = [k.key, k.values.get(value)];

              this.emit("invert_data", ret);

              if (outputStream) outputStream.write(ret);
              if (cb) cb(errors, ret);
              if (!outputStream && !cb) results.push(ret);
            } else
              errors.push(
                `Value ${value} for Key ${key} is not in the Model. Maybe you should refit it.`
              );
          } else errors.push(`Key ${key} is not in the Model. Maybe you should refit it.`);
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

  extractDataFromModel(raw) {
    const decompressed = unzipSync(raw);
    const model = eval("(" + decompressed + ")");

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
    this.tokenSeparator = model.tokenSeparator;
    this.autoupdate = model.autoupdate;
    this.autoupdateFactor = model.autoupdateFactor;

    return model;
  }

  getModel() {
    if (this.model) {
      const model = clone.default(this.model);
      model.name = this.name;
      model.fields = this.fields;
      model.skipFields = this.skipFields;
      model.fillNA = this.fillNA;
      model.uniformTokenLength = this.uniformTokenLength;
      model.NA = this.NA;
      model.tokenSeparator = this.tokenSeparator;
      model.keyAlphabet = this.keyAlphabet;
      model.valuesAlphabet = this.valuesAlphabet;
      model.filterData = this.filterData;
      model.translateKey = this.translateKey;
      model.translateValue = this.translateValue;
      model.autoupdate = this.autoupdate;
      model.autoupdateFactor = this.autoupdateFactor;
      model.creationDate = Date.now();

      const serialized = serialize.default(model);
      const compressed = gzipSync(Buffer.from(serialized));

      Object.keys(model.translators).forEach((key) => {
        model.translators[key].values = [...model.translators[key].values];
      });
      Object.keys(model.inverters).forEach((key) => {
        model.inverters[key].values = [...model.inverters[key].values];
      });

      return [compressed, model];
    }
  }

  saveModel({ dir, generateJSONCopy = false }) {
    if (!dir) throw new Error("No Path to Save Model");

    if (this.model) {
      const [compressed, model] = this.getModel();

      if (!dir.endsWith("/")) dir = `${dir}/`;
      if (generateJSONCopy) {
        fs.writeFileSync(`${dir}${model.name}.json`, JSON.stringify(model, null, 1));
      }

      fs.writeFileSync(`${dir}${model.name}.j2s`, compressed);

      this.emit("model_save", compressed, model);
      return [compressed, model];
    } else throw new Error("No Model To Save");
  }

  loadModel(path) {
    const model = this.extractDataFromModel(fs.readFileSync(path));

    if (this.verbose) console.log(model);

    this.model = model;
    this.emit("model_loaded", model);
  }

  setModel(raw) {
    const model = this.extractDataFromModel(raw);

    if (this.verbose) console.log(model);

    this.model = model;
    this.emit("model_set", model);
  }
}
