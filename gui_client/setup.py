from setuptools import find_packages, setup

package_name = 'gui_client'

setup(
    name=package_name,
    version='0.1.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml', 'resource/plugin.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='mackop',
    maintainer_email='maciej.p.krupka@student.put.poznan.pl',
    description='RQt GUI client for interacting with the introspection manager.',
    license='Apache License 2.0',
    tests_require=['pytest'],
    entry_points={
        'rqt_gui_py.plugins': [
            'introspection_gui = gui_client.plugin:IntrospectionGuiPlugin',
        ],
    },
)
