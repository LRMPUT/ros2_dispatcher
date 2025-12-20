from setuptools import find_packages, setup

package_name = 'gui_client'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        ('share/' + package_name, ['plugin.xml']),
    ],
    install_requires=['setuptools', 'pyyaml', 'dispatcher_controller'],
    zip_safe=True,
    maintainer='mackop',
    maintainer_email='maciej.p.krupka@student.put.poznan.pl',
    description='TODO: Package description',
    license='TODO: License declaration',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
        ],
        'rqt_gui_py.plugin': [
            'introspection_gui = gui_client.rqt_gui_py:IntrospectionPlugin',
        ],
    },
)
