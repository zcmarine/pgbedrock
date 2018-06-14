import pytest
import yaml

from pgbedrock import constructor


class MyLoader(yaml.Loader):
    """
    Subclass yaml.Loader so we don't modify the real yaml.Loader (that may have effects
    that outlast this test)
    """
    def __init__(self, *args, **kwargs):
        self.root_was_processed = False
        super(MyLoader, self).__init__(*args, **kwargs)


def run_spec_test(function_to_test, spec, expected):
    """
    Given a function and a spec, generate the constructor we will use, skip the root node, and
    assert that our spec passes our function's assertion
    """
    def hookin_constructor(loader, node):
        """ A constructor to get access to a set of YAML nodes so we can test our function """
        # Construct the document root normally
        if not loader.root_was_processed:
            loader.root_was_processed = True
            return yaml.constructor.Constructor.construct_yaml_map(loader, node)

        # What we're actually testing
        assert function_to_test(node) == expected

    MyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, hookin_constructor)
    yaml.load(spec, Loader=MyLoader)


@pytest.mark.parametrize('spec, expected', [
    ("""
    tables:
        write:
            - foo
            - bar
        read:
            - baz
    """, True),

    ("""
     tables:
        write:
            - foo
    """, True),

    ("""
     tables:
        read:
            - foo
    """, True),

    ("""
     tables:
        read:
    """, True),

    ("""
     tables:
    """, True),

    ("""
    tables:
        write:
            - foo
            - bar
        something_with_read_in_its_name:
            - baz
    """, False),

    ("""
     tables:
        something_else:
            - foo
    """, False),
])
def test_has_only_read_write_keys(spec, expected):
    run_spec_test(
        function_to_test=constructor.has_only_read_write_keys,
        spec=spec,
        expected=expected
    )


@pytest.mark.parametrize('spec, expected', [
    ("""
     tables:
         - foo
         - bar
     schemas:
         - baz
     sequences:
         - qux
     """, True),

    ("""
     tables:
     """, True),

    ("""
     tables:
         - foo
     something_with_schemas_in_its_name:
         - bar
     """, False),

    ("""
     something_with_schemas_in_its_name:
         - bar
     """, False),
])
def test_all_children_are_object_kinds(spec, expected):
    run_spec_test(
        function_to_test=constructor.all_children_are_object_kinds,
        spec=spec,
        expected=expected
    )


@pytest.mark.parametrize('spec, expected', [
    ("""
     - foo
     - bar
     """, True),

    ("""
     - foo
     """, True),

    ("""
     foo: bar
     """, False),

    ("""
     foo
     """, False),
])
def test_all_children_are_sequence_nodes(spec, expected):
    run_spec_test(
        function_to_test=constructor.all_children_are_sequence_nodes,
        spec=spec,
        expected=expected
    )


def test_objectname_constructor():
    def hookin_constructor(loader, node):
        """ A constructor to get access to a set of YAML nodes so we can test our function """
        # Construct the document root normally
        if not loader.root_was_processed:
            loader.root_was_processed = True
            return yaml.constructor.Constructor.construct_yaml_map(loader, node)

        return constructor.objectname_constructor(loader, node)

    spec = """
        owns:
            schemas:
                - cdean_revrec_validation
                - deprecated_bi_deleted_tables_remove_after_20180529
            tables:
                - deprecated_bi_deleted_tables_remove_after_20180529.*
                - metadata."pg_stat_statements"
                - partman.*
                - repack.*
                - staging_archive."stripe_events_sta"
        """
    # MyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, constructor.objectname_constructor)
    MyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, hookin_constructor)
    output = yaml.load(spec, Loader=MyLoader)
    import pdb; pdb.set_trace()
    1+1
