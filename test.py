from visualizer.map_empty_parcels import map_empty_civ_addresses
from query_tools.object_finders import find_parcel_covers
from pprint import pformat
from query_tools.general_query import query_endpoint
from objects.objects import ALL_DATA_GENERAL
from copy import deepcopy

if __name__ == '__main__':
    # map_empty_civ_addresses()

    complete_response = ''
    

    for zoning_object in find_parcel_covers("cot:Property342127", "hp:ZoningType"):
        partial = query_endpoint(ALL_DATA_GENERAL.replace("CUSTOM_PROPERTY_OBJ", zoning_object))["results"]["bindings"]
        to_add = deepcopy(partial)

        #Remove polys
        for idx, entry in enumerate(partial):
            for object in entry.values():
                try:
                    if "POLYGON" in object['value']: 
                        del to_add[idx]
                        print(object['value'])
                except:
                    pass

        complete_response += f"\n\n{pformat(to_add)}"

    quit()