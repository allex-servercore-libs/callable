module.exports = function (lib) {
  'use strict';
  var q = lib.q,
    jsonschema = lib.jsonschema,
    validator = new (jsonschema.Validator)();
  //TODO: load some prefefined schemas into validator for common data types like email and so on ...
  function extractValidationMessages(title, error) {
    return (title ? (title + ': ') : '') + error.message;
  }

  function validateSingleParam(errors, complete_schema, param_value, param_index) {
    var schema = complete_schema[param_index], ret, result;
    if( lib.isUndef(schema) ){
      console.error('no schema for',param_index,'in',complete_schema);
      return undefined;
    }

    //if schema === true just let it pass ...
    if (schema === true) {
      return undefined;
    }
    if ((null === param_value || lib.isUndef(param_value)) && schema.required===false) {
      ret = schema.required ? 'Missing param: ' + schema.title : undefined;
      //console.log('Param value is', param_value, 'and ', schema.required, ret);
      return ret;
    }
    result = validator.validate(param_value, schema);
    if (!result.errors.length) {
      return;
    }
    Array.prototype.push.apply(errors, result.errors.map(extractValidationMessages.bind(null, schema.title)));
  }

  var __id=0;
  function Callable() {
    //this.__id = ++__id;
    this._activeDefers = new lib.DeferMap();
  }
  Callable.prototype.destroy = function () {
    //console.trace();
    //console.log(this.__id, 'destroying');
    var ad = this._activeDefers;
    this._activeDefers = null;
    if (ad) {
      ad.destroy();
    }
  };
  Callable.prototype.executeStep = function (stepspec) {
    //console.log('stepspec', stepspec);
    var ret = this._activeDefers.defer(lib.uid()),
      methodname = stepspec[0],
      method = this[methodname],
      ml,
      params,
      trailing,
      vp,
      cleaner;
    if (!ret.resolve) {
      console.trace();
      console.error('this has to be a defer:', ret);
      process.exit(0);
    }
    if ('function' !== typeof method) {
      console.trace();
      console.log('In', this.__methodDescriptors, 'Method ' + methodname + ' does not exist');
      ret.reject(new lib.Error('METHOD_DOES_NOT_EXIST', 'Method ' + methodname + ' does not exist'));
      return ret.promise;
    }
    if (this.__methodDescriptors && !this.__methodDescriptors[methodname]) {
      console.log(this.__methodDescriptors, methodname, '?');
      ret.reject(new lib.Error('METHOD_NOT_EXECUTABLE', 'Method ' + methodname + ' is not executable'));
      return ret.promise;
    }
    ml = method.length - 1;
    if (ml < 0) {
      console.error(this[methodname].toString(), 'is not callable');
      ret.reject(new lib.Error('Method ' + methodname + ' is not callable'));
      return ret.promise;
    }
    params = stepspec[1] || [];
    if (!(params instanceof Array)) {
      params = [params];
    }
    while (params.length < ml) {
      params.push(void 0);
    }
    while (params.length > ml) {
      params.pop();
    }
    //console.log('stepspec', stepspec, 'params', params);
    if (this.__methodDescriptors && this.__methodDescriptors[methodname]) {
      if (lib.isArray(this.__methodDescriptors[methodname])) {
        if (params.length !== this.__methodDescriptors[methodname].length) {
          ret.reject(new lib.Error('RMI_ARGUMENT_COUNT_MISMATCH', 'Method ' + methodname + ' has arguments length mismatch, expected '+this.__methodDescriptors[methodname].length+' parameter, got '+params.length+' parameter . Params are: '+JSON.stringify(params)));
          return ret.promise;
        }
      }

      vp = Callable.validateParams(params, this.__methodDescriptors[methodname]);
      if (vp) {
        console.trace();
        console.error(methodname, 'should reject params', params, 'due to', vp);
        ret.reject(new lib.Error(vp));
        return ret.promise;
      }
    }
    params.push(ret);
    //hack for communication injection
    trailing = stepspec[2];
    if('undefined' !== typeof trailing){
      if (lib.isArray(trailing)){
        trailing.forEach(function(t){
          params.push(t);
        });
      } else {
        params.push(stepspec[2]);
      }
    }
    try {
      method.apply(this, params);
    } catch (e) {
      console.log(e.stack);
      ret.reject(e);
    }
    params = null;
    return ret.promise;
  };
  Callable.prototype.exec = function (callspec) {
    if ('object' !== typeof callspec) {
      return q.reject(new lib.Error('NOT_A_CALLABLE_FORMAT', 'Not a callable format'));
    }
    if (!this._activeDefers) {
      return q.reject(new lib.Error('CALLABLE_DESTROYING', 'Callable is in destruction'));
    }
    return this.executeStep(callspec);
  };
  Callable.validateParams = function (params, validationschema) {
    if (!lib.isArray(validationschema)) {
      ///if validationschema is not an array, and expression is true, count it as ok ...
      return validationschema ? undefined : 'failed';
    }
    var errors = [], _errors = errors, _vs = validationschema;
    params.forEach(validateSingleParam.bind(null, _errors, _vs));
    _errors = null;
    _vs = null;
    return errors.length ? errors.join("\n") : undefined;
  };
  function augmentObj(obj, item, name) {
    obj[name] = item;
  }
  Callable.inherit = function (userChildCtor, methodDescriptors) {
    lib.inherit(userChildCtor, this);
    userChildCtor.prototype.__methodDescriptors = {};
    var md = userChildCtor.prototype.__methodDescriptors, bao = augmentObj.bind(null, md);
    lib.traverse(this.prototype.__methodDescriptors, bao);
    lib.traverse(methodDescriptors, bao);
    md = null;
    userChildCtor.inherit = this.inherit;
  };
  return Callable;
};
