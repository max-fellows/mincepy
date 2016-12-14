# MincePy
from AbstractQueryFeature import AbstractQueryFeature
from UnsupportedQueryFeatureError import UnsupportedQueryFeatureError
from IncompleteTemplateError import IncompleteTemplateError

class TemplatedQueryFeature(AbstractQueryFeature):
    '''
    Enables string interpolation support in queries. An external script is used
    to construct the dictionary used for interpolation; this can be treated as
    a configuration step outside the scope of the MincePy project. The
    external script, or "template provider," must contain the following method
    which returns a dictionary of string substitutions:
        
        getStrings(db)
        
    :param templateProvider: the template provider, imported as a module
    :type templateProvider: Python module
    '''
    
    def __init__(self, templateProvider):
        self.__templateProvider = templateProvider
        
    
    def prepareQuery(self, sql, featureProvider):
        if not featureProvider:
            raise UnsupportedQueryFeatureError(self)
        
        db = featureProvider.getDBConnection()
        
        try:
            return sql % self.__templateProvider.getStrings(db)
        except KeyError as e:
            raise IncompleteTemplateError(self.__templateProvider, str(e), sql)
