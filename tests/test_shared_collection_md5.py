"""Tests for the md5 re-validation bypass of ``import_shared_collection``.

``aiida-pseudo``'s ``PseudoPotential.store()`` calls ``validate_md5()``, which
downloads the full file from the repository backend. The metadata-only import
bypasses that override by invoking the parent ``store()`` implementation
directly (no monkey-patching: the import runs on worker threads alongside
other activities in the same process). These tests verify the class
resolution and the bypass mechanics, without needing a storage backend.
"""

import pytest

pytest.importorskip('aiida_pseudo')

from aiida import orm
from aiida_pseudo.data.pseudo import upf

from aiida_s3 import shared_collection
from aiida_s3.shared_collection import _pseudo_base_class, _store_node


def test_pseudo_base_class_resolution():
    """The base class is found for pseudo classes, not for anything else."""
    assert _pseudo_base_class(upf.UpfData) is not None
    assert _pseudo_base_class(upf.UpfData) in upf.UpfData.__mro__
    assert _pseudo_base_class(orm.Dict) is None
    assert _pseudo_base_class(orm.SinglefileData) is None


def test_store_node_bypasses_pseudo_validation(monkeypatch):
    """A pseudo node stores via the parent implementation: the validating
    override (which re-downloads the file) is never called."""
    order = []

    class GrandParent:
        def store(self):
            order.append('stored')
            return self

    class Parent(GrandParent):
        """Plays the aiida-pseudo base: store() re-validates the md5."""

        def validate_md5(self, md5):
            order.append('downloaded')
            raise AssertionError('the file content must not be read')

        def store(self, **kwargs):
            self.validate_md5(self.md5)
            return super().store()

    class Child(Parent):
        """Plays UpfData."""

    monkeypatch.setattr(
        shared_collection, '_pseudo_base_class', lambda cls: Parent if issubclass(cls, Parent) else None
    )

    node = Child()
    node.md5 = 'recorded-in-the-manifest'
    _store_node(node, Child)

    assert order == ['stored']


def test_store_node_plain_class_stores_normally(monkeypatch):
    """A non-pseudo class goes through its own store(), untouched."""
    monkeypatch.setattr(shared_collection, '_pseudo_base_class', lambda cls: None)

    class Plain:
        def store(self):
            self.stored = True
            return self

    node = Plain()
    _store_node(node, Plain)
    assert node.stored


def test_bypass_does_not_touch_class_state(monkeypatch):
    """No method of any class is replaced: the bypass holds no global state."""
    from aiida_pseudo.data import pseudo as pseudo_module

    before = {
        name: getattr(pseudo_module, name).validate_md5
        for name in ('PseudoPotentialData', 'PseudoPotential')
        if hasattr(pseudo_module, name)
    }

    class GrandParent:
        def store(self):
            return self

    class Parent(GrandParent):
        def validate_md5(self, md5):
            raise AssertionError('must not be called')

        def store(self):
            self.validate_md5('md5')
            return super().store()

    class Child(Parent):
        pass

    monkeypatch.setattr(shared_collection, '_pseudo_base_class', lambda cls: Parent)

    _store_node(Child(), Child)

    for name, method in before.items():
        assert getattr(pseudo_module, name).validate_md5 is method
