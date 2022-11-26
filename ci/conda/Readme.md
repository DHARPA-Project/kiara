This folder contains the patch files needed to build conda packages for the 'kiara' package and its dependencies that are not already hosted on conda-forge.

The command to build and publish the packages is:

```
kiara conda build-package --user dharpa --publish --patch-data ci/conda/{{ package_name }}/conda-pkg-patch.yaml {{ package_name }}
```
