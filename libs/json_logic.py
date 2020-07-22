import json

from config.constants import (COMPARATORS, FREE_RESPONSE, FREE_RESPONSE_NUMERIC,
    NUMERIC_COMPARATORS, NUMERIC_QUESTIONS)


class InvalidLogicError(Exception): pass  # this is the super class, it should not show up in any validation check
class NonExistantUUIDReference(InvalidLogicError): pass  # the uuid referenced does not exist
class InvalidOperator(InvalidLogicError): pass  # the comparator provided is not a valid comparator
class InvalidNumeric(InvalidLogicError): pass  # the value provided (by the researcher) to compare against is not numeric
class NumericPointerInvalid(InvalidLogicError): pass  # the answer pointed to should be of a numeric answer type but isn't
class QuestionReferenceOutOfOrder(InvalidLogicError): pass  # this question references a question that comes after it
class EmptyLogicObject(InvalidLogicError): pass  # this question has an empty logic object


error_messages = {
    str(NonExistantUUIDReference().__class__):"contains a reference to a question that does not exist.",
    str(InvalidOperator().__class__):"contains a logic operator that is not supported.",
    str(InvalidNumeric().__class__):"contains a value you entered that is not numeric.",
    str(NumericPointerInvalid().__class__):"contains a reference to a question that is not of a numeric question type.",
    str(QuestionReferenceOutOfOrder().__class__):"contains a reference to another question that occurs later in the survey.",
    str(EmptyLogicObject().__class__):"contains a section without any logic in it.",
}


def validate_survey_json(json_survey_string):
    return do_validate_survey( json.loads(json_survey_string) )


def validate_survey_from_db(survey):
    return do_validate_survey(survey['content'])


def do_validate_survey(questions):
    # The existence of this key is used to distinguish validation errors from other errors
    errors = {"duplicate_uuids": []}
    
    # determine duplicate question ids
    question_ids = set()
    for question in questions:
        uuid = question['question_id']
        if question['question_id'] in question_ids:
            errors['duplicate_uuids'].append(uuid)
        question_ids.add(uuid)
        
    # get mapping of uuids to questions...
    questions_dict = {question['question_id']:question for question in questions}
    
    questions_validated = set()
    for question in questions:
        questions_validated.add(question['question_id'])
        try:
            if "display_if" in question:  # if there is no display logic we will display it in the app.
                validate_logic_tree(question['display_if'], questions_dict, questions_validated)
        except InvalidLogicError as e:
            errors[question['question_id']] = [error_messages[str(e.__class__)], str(e)]
    return errors


def validate_logic_tree(logic_entry, questions_dict, questions_validated):
    """ Structure and vocab:
    A logic entry is a single key dictionary, with allowed keys being the logic operators and comparators.
    The value associated with that key depends on the value of the key.
    operators (and, or): must have a list of logic entries, e.g. [{logic entry 1}, {logic entry 2}]
    comparators (<,>,==,!=): must have a list of 2 values to compare
    invert (not): simply has the inner logic entry, eg: { not: {logic entry} }, and it inverts the output.
    """

    # case: top logic object is None, this means it will display; return
    # (this case should be the same as "display if" not being in the survey object)
    if logic_entry is None:
        return
    
    # case: the logic object is empty. (Pretty sure has never triggered)...
    if len(logic_entry) == 0:
        raise EmptyLogicObject(logic_entry)
    
    # case: too many keys (the above case catches 0 keys)
    operators = list(logic_entry.keys())
    if len(operators) != 1:
        raise InvalidOperator(operators)

    operator = operators[0]  # just grabbing the key outright to simplify code.
    # any time you see logic_entry[operator] that means we are grabbing the next level in the object

    # case: the operator points to an empty list or dictionary or None. That is invalid
    if not logic_entry[operator]:
        raise EmptyLogicObject(operator)
    
    if operator == 'or' or operator == "and": # handle the container types "or" and "and"
        for l in logic_entry[operator]:
            validate_logic_tree(l, questions_dict, questions_validated)
        return
    
    if operator == "not":  # handle the container type "not"
        validate_logic_tree(logic_entry[operator], questions_dict, questions_validated)
        return
    
    if operator in COMPARATORS:  # handle the (numerical) logical operators
        validate_logic_entry(logic_entry, questions_dict, questions_validated)
        return
    
    raise InvalidOperator(operator)


def validate_logic_entry(logic_entry, questions_dict, questions_validated):
    comparator = list(logic_entry.keys())[0]
    uuid, comparator_value = list(logic_entry.values())[0]
    
    # case: uuid does not exist anywhere.
    if uuid not in questions_dict:
        raise NonExistantUUIDReference(uuid)
    
    # case: uuid is not in any prior question. (this + prior check = exists in future question)
    if uuid not in questions_validated:
        raise QuestionReferenceOutOfOrder(uuid)
    
    # case: the numerical value entered by the admin is not a numeric.
    if comparator in NUMERIC_COMPARATORS:
        try:
            float(comparator_value)
        except ValueError:
            raise InvalidNumeric(comparator_value)
    
    # case: conditional logic references another question that is not a numeric type question.
    question_type = questions_dict[uuid]['question_type']
    if (question_type not in NUMERIC_QUESTIONS
        or (question_type == FREE_RESPONSE
            and questions_dict[uuid]["text_field_type"] != FREE_RESPONSE_NUMERIC)):
        raise NumericPointerInvalid(uuid)
        
###############################################################################################
##########  THIS IS NEVER GOING TO BE PRODUCTION CODE, IT IS FOR REFERENCE ONLY. ##############
###############################################################################################
#
# class BadOperatorError (Exception): pass
# class BadLogicError (Exception): pass
#
# def logic_tree_parse(logic_entry):
#     """ Structure and vocab:
#         A logic entry is a single key dictionary, with allowed keys being the logic operators and comparators.
#         The value associated with that key depends on the value of the key.
#
#         operators (and, or) must have a list of logic entries, e.g. [{logic entry 1}, {logic entry 2}]
#
#         comparators (<,>,==,!=) must have a list of 2 values to compare
#
#         invert (not) simply has the inner logic entry, eg: { not: {logic entry} }, and it inverts the output.
#         """
#     operators = logic_entry.keys()
#     if len(operators) != 1:
#         raise BadLogicError("multiple keys: %s" % operators)
#
#     operator = operators[0]
#     #any time you see logic_entry[operator] that means we are grabbing the next level in the object
#
#     if operator == 'or':
#         return any(logic_tree_parse(l) for l in logic_entry[operator])
#
#     if operator == 'and':
#         return all(logic_tree_parse(l) for l in logic_entry[operator])
#
#     if operator == "not":
#         return not logic_tree_parse(logic_entry[operator])
#
#     if operator in COMPARATORS:
#         return do_logic(logic_entry)
#
#     raise InvalidComparator(operator)
#
#
# def do_logic(logic_entry):
#
#     comparator = list(logic_entry.keys())[0]
#     a, b = list(logic_entry.values())[0]
#     #code no longer functional, a is always a uuid value.
#     if comparator in NUMERIC_COMPARATORS:
#         a = float(a)
#         b = float(b)
#
#     if comparator == "<":
#         return a < b
#
#     if comparator == ">":
#         return a > b
#
#     if comparator == "<=":
#         return a <= b
#
#     if comparator == ">=":
#         return a >= b
#
#     if comparator == "==":
#         return a == b
#
#     if comparator == "!=":
#         return a != b
#
#     raise BadOperatorError("invalid operator (2): %s" % comparator)
#
# assert(logic_tree_parse({"<": [1, 2]}))
# assert(not logic_tree_parse({"<": [2, 1]}))
#
# assert(logic_tree_parse({">": [2, 1]}))
# assert(not logic_tree_parse({">": [1, 2]}))
#
# assert(logic_tree_parse({"<=": [1, 2]}))
# assert(not logic_tree_parse({"<=": [2, 1]}))
#
# assert(logic_tree_parse({"<=": [1, 2]}))
# assert(not logic_tree_parse({"<=": [2, 1]}))
#
# assert(logic_tree_parse({"==": [1, 1]}))
# assert(not logic_tree_parse({"==": [2, 1]}))
#
# assert(logic_tree_parse({"!=": [2, 1]}))
# assert(not logic_tree_parse({"!=": [1, 1]}))
#
# #test not
# x={"not": {'==': [1,1] }} #false
# assert(not logic_tree_parse(x))
#
# x={"not": {'!=': [1,1] }} #true
# assert(logic_tree_parse(x))
#
# #test and
# x={"and":[
#     {'==': [1,1] }, #true
#     {'!=': [1,2] }  #true
# ]} #result should be true
# assert(logic_tree_parse(x))
#
# x={"and":[
#     {'==': [1,1] }, #true
#     {'!=': [1,1] }  #false
# ]} #result should be false
# assert(not logic_tree_parse(x))
#
# x={"and":[
#     {'==': [1,2] }, #false
#     {'!=': [1,2] }  #true
# ]} #result should be false
# assert(not logic_tree_parse(x))
#
# x={"and":[ #single value
#     {'==': [1,1] } #true
# ]} #result should be true
# assert(logic_tree_parse(x))
#
# x={"and":[ #single value
#     {'==': [1,2] } #false
# ]} #result should be false
# assert(not logic_tree_parse(x))
#
# #test or
# x={"or":[
#     {'==': [1,1] }, #true
#     {'!=': [1,1] }  #false
# ]} #result should be true
# assert(logic_tree_parse(x))
#
# x={"or":[
#     {'==': [1,2] }, #false
#     {'!=': [1,2] }  #true
# ]} #result should be true
# assert(logic_tree_parse(x))
#
# x={"or":[
#     {'==': [1,2] }, #false
#     {'!=': [1,1] }  #false
# ]} #result should be false
# assert(not logic_tree_parse(x))
#
# x={"or":[ #single value
#     {'==': [1,1] } #true
# ]} #result should be true
# assert(logic_tree_parse(x))
#
# x={"or":[ #single value
#     {'==': [1,2] } #false
# ]} #result should be false
# assert(not logic_tree_parse(x))
