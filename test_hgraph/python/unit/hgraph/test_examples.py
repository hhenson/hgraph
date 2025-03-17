from examples.component.component_example import main as component_main
from examples.first.trivial import main as trivial_main
from examples.first.trivial_tsd import main as trivial_tsd_main
from examples.mesh.mesh_example import main as mesh_main


def test_examples():
    component_main()
    trivial_main()
    trivial_tsd_main()
    mesh_main()
