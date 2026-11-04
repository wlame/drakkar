"""The count of this worker's current readers of each shared-folder file."""

from drakkar.uiserver.files_in_use import FilesInUse


def test_a_held_path_is_in_use_until_its_release():
    files = FilesInUse()

    release = files.hold(['merged/a.db', 'merged/b.db'])
    assert release is not None

    assert files.in_use('merged/a.db') and files.in_use('merged/b.db')
    assert not files.in_use('merged/c.db')
    release()
    assert not files.in_use('merged/a.db') and not files.in_use('merged/b.db')


def test_two_holders_keep_a_path_in_use_until_both_release():
    files = FilesInUse()
    first = files.hold(['merged/a.db'])
    second = files.hold(['merged/a.db'])
    assert first is not None and second is not None

    first()
    assert files.in_use('merged/a.db')
    second()
    assert not files.in_use('merged/a.db')


def test_a_second_release_and_a_path_named_twice_count_once():
    files = FilesInUse()
    other = files.hold(['merged/a.db'])
    release = files.hold(['merged/a.db', 'merged/a.db'])
    assert other is not None and release is not None

    release()
    release()

    assert files.in_use('merged/a.db')
    other()
    assert not files.in_use('merged/a.db')


def test_holding_nothing_changes_nothing():
    files = FilesInUse()

    release = files.hold([])
    assert release is not None
    release()

    assert not files.in_use('merged/a.db')


def test_a_file_being_deleted_refuses_new_holds_until_the_delete_ends():
    files = FilesInUse()
    end = files.begin_delete((1, 2))

    assert files.hold([(1, 2), (1, 3)]) is None
    assert not files.in_use((1, 3))
    end()
    release = files.hold([(1, 2)])
    assert release is not None and files.in_use((1, 2))
