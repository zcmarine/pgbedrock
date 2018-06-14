import yaml

from pgbedrock.context import PRIVILEGE_MAP


def all_children_are_object_kinds(node):
    children = set([child.value for child in node.value])
    if children.difference(PRIVILEGE_MAP.keys()):
        return False
    return True


def has_only_read_write_keys(node):
    node_keys = set([child[0].value for child in node.value])
    reads_and_writes = set(['read', 'write'])
    return node_keys.difference(reads_and_writes) == set()


def all_children_are_sequence_nodes(node):
    for child_key, child_values in node.value:
        if child_values.id != yaml.resolver.BaseResolver.DEFAULT_SEQUENCE_TAG:
            return False

    return True


class CustomLoader(yaml.Loader):
    """ A copy of yaml.Loader so we can be sure we don't screw up any default YAML machinery """
    pass


def objectname_constructor(loader, node):
    """
    Built using SafeConstructor.construct_yaml_map as a base, as defined here:
        https://github.com/anishathalye/pyyaml/blob/2c225b29fc6daeb343b65d0ca30ba1c7ad297c77/lib3/yaml/constructor.py#L395-L399

    We convert to ObjectNames any sequence items belonging to the mapping nodes named:
        - 'read' or 'write', as these are the lowest mapping nodes in the privileges subdict
        - 'schemas', 'tables', 'sequences', or any other key in context.PRIVILEGE_MAP, since if
            these hold sequence nodes then they are the lowest mapping nodes in the owns subdict
    """
    # if all children are mappings with values in ('read', 'write'), then populate
    # elif all children are in PRIVILEGE_MAP.keys() and their children are all sequences, then # populate
    if True:
        import pdb; pdb.set_trace()
    else:
        # Dispatch to the default mapping constructor
        return yaml.constructor.Constructor.construct_yaml_map(loader, node)


    # if node.value and has_only_read_write_keys(node) and all_children_are_sequence_nodes(node):
    #     import pdb; pdb.set_trace()
    # else:
    #     return yaml.constructor.Constructor.construct_yaml_map(loader, node)

    # if set([child[0].value for child in node.value]).difference(set(['read', 'write'])):
    #     return yaml.constructor.Constructor.construct_yaml_map(loader, node)
    # else:
    #     import pdb; pdb.set_trace()


    # import pdb; pdb.set_trace()
    # # [child[0].value for child in node.value]
    # data = {}
    # yield data
    # value = loader.construct_mapping(node)
    # data.update(value)

def convert_spec_to_objectnames(spec):
    """ Convert object names in a loaded spec from strings to ObjectName instances

    This converts items in the following sublists, if those sublists exist:
        * <role_name> -> owns -> <key in context.PRIVILEGE_MAP>
        * <role_name> -> privileges -> <key in context.PRIVILEGE_MAP> -> read
        * <role_name> -> privileges -> <key in context.PRIVILEGE_MAP> -> write
    """
    for role, config in spec.items():
        for objkind, owned_items in config.get('owns', {}):
            # Do things
            pass

        for objkind, perm_dicts in config.get('privileges', {}):
            for priv_kind, granted_items in perm_dicts.items():
                # Do things
                pass

    return spec
