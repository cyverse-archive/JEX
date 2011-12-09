var _ = require('underscore');


/*
 *  Creates and returns a function that compares the
 *  parameter passed into it with the character passed
 *  into this function.
 *
 *  Mainly used so _.any() is comfortable to use against
 *  strings.
 *
 *  Params:
 *      chr2 - the character to match against.
 *  Returns:
 *      A function that returns a boolean if the character
 *      passed into it matches the chr2.
 */
var chars_match = function (chr2) {
    return function (chr) {
        return chr === chr2;
    };
};

var has_space = function (arg) {
    return arg.indexOf(" ") != -1;
};

var has_single_quote = function (arg) {
    return arg.indexOf("'") != -1;
};

var has_double_quote = function (arg) {
    return arg.indexOf('"') != -1;
};

var double_single_quotes = function (arg) {
    return arg.replace(/\'/g, "''");
};

var triple_single_quotes = function (arg) {
    return arg.replace(/\'/g, "'''");
};

var double_double_quotes = function (arg) {
    return arg.replace(/\"/g, '""');
};

var wrap_in_double_quotes = function (arg) {
    return '"' + arg + '"';
};

/*
 *  Escapes an arg so it can be used with Condor.
 *  
 *  See http://www.cs.wisc.edu/condor/manual/v7.0/condor_submit.html
 *  for more info.
 */
var escape = function (arg) {
    var new_arg = arg;
    var wrap_in_single = false;
    
    //If the arg contains spaces, then the entire arg needs to
    //be wrapped in single quotes. But that needs to happen
    //last.
    if (has_space(new_arg) || has_single_quote(new_arg)) {
        wrap_in_single = true;
    }
    
    //If the arg has spaces and contains single quotes, then
    //we need to double the single quotes.
    if (wrap_in_single && has_single_quote(new_arg)) {
        new_arg = double_single_quotes(new_arg);
    } else {
        //If the arg doesn't have spaces but does have single quotes,
        //then we need to double the single quotes and wrap the arg.
        if (!wrap_in_single && has_single_quote(new_arg)) {
            wrap_in_single = true;
            new_arg = double_single_quotes(new_arg);
        }
    }
    
    //Double quotes need to be doubled.
    if (has_double_quote(new_arg)) {
        new_arg = double_double_quotes(new_arg);
    }
    
    //Make sure that the arg is wrapped in single-quotes.
    if (wrap_in_single) {
        new_arg = "'" + new_arg + "'";
    }
    
    return new_arg;
};

/*
 *  Creates a Condor-escaped argument string from a list
 *  of un-escaped arguments.
 *  
 *  Params:
 *      args - a list of strings containing unescaped arguments.
 *  Returns:
 *      A Condor-escaped string containing the arguments.
 */
var make_condor_args = function (args) {
    var escaped_args = _.map(args, escape).join(" ");
    return wrap_in_double_quotes(escaped_args);
};

exports.chars_match = chars_match;
exports.has_space = has_space;
exports.has_single_quote = has_single_quote;
exports.has_double_quote = has_double_quote;
exports.double_single_quotes = double_single_quotes;
exports.triple_single_quotes = triple_single_quotes;
exports.double_double_quotes = double_double_quotes;
exports.wrap_in_double_quotes = wrap_in_double_quotes;
exports.escape = escape;
exports.make_condor_args = make_condor_args;

