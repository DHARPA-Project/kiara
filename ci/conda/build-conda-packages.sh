#!/usr/bin/env bash

# adopted from: https://github.com/MichaelsJP/conda-package-publish-action
# License: MIT

set -ex
set -o pipefail

INPUT_SUBDIR=$1
CONDA_PACKAGE_VERSION=$2
PYTHON_VERSION=$3
INPUT_DRY_RUN=$4

if [ -z "$INPUT_DRY_RUN" ]; then
  INPUT_DRY_RUN="true"
fi

echo $INPUT_DRY_RUN

ANACONDA_USER="dharpa"
INPUT_OVERRIDE="false"
INPUT_PLATFORMS="all"

CHANNEL_ARGS="-c conda-forge -c dharpa"


BUILD_ROOT="build_${PYTHON_VERSION}"


go_to_build_dir() {
    if [ ! -z $INPUT_SUBDIR ]; then
        cd $INPUT_SUBDIR
    fi
}

check_if_meta_yaml_template_file_exists() {
    if [ ! -f meta.yaml.template ]; then
        echo "meta.yaml.template must exist in the directory that is being packaged and published."
        exit 1
    fi
}

create_meta_yaml() {

    if [ -z $PYTHON_VERSION ]; then
        echo "No python version specified."
        exit 1
    fi

    if [ -z $CONDA_PACKAGE_VERSION ]; then
        echo "No package version specified."
        exit 1
    fi

    mkdir -p "${BUILD_ROOT}"
    echo "# CREATING 'meta.yaml'..."
    sed "s/__VERSION__/${CONDA_PACKAGE_VERSION}/" meta.yaml.template > "${BUILD_ROOT}/meta.yaml"
}

build_package(){

    cd "${BUILD_ROOT}"

    local build_folder="build_data"
    echo "# CREATING BUILD FOLDER..."
    rm -rf "${build_folder}"

    # Build for Linux
    echo "# BUILDING PACKAGE..."
    conda mambabuild --py ${PYTHON_VERSION} ${CHANNEL_ARGS} --output-folder "${build_folder}" .

    cd "${build_folder}"

    # Convert to other platforms
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"osx-64"* ]]; then
      echo "# CONVERTING PACKAGE FOR PLATFORM 'osx-64'..."
      conda convert -p osx-64 linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"osx-arm64"* ]]; then
      echo "# CONVERTING PACKAGE FOR PLATFORM 'osx-arm64'..."
      conda convert -p osx-arm64 linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-32"* ]]; then
      echo "# CONVERTING PACKAGE FOR PLATFORM 'linux-64'..."
      conda convert -p linux-32 linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-ppc64"* ]]; then
      echo "# CONVERTING PACKAGE FOR PLATFORM 'linux-ppc64'..."
      conda convert -p linux-ppc64 linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-ppc64le"* ]]; then
      echo "# CONVERTING PACKAGE FOR PLATFORM 'linux-ppc641e'..."
      conda convert -p linux-ppc64le linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-s390x"* ]]; then
      echo "# CONVERTING PACKAGE FOR PLATFORM 'linux-x390x'..."
      conda convert -p linux-s390x linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-armv6l"* ]]; then
      echo "# CONVERTING PACKAGE FOR PLATFORM 'linux-armv6l'..."
      conda convert -p linux-armv6l linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-armv7l"* ]]; then
      echo "# CONVERTING PACKAGE FOR PLATFORM 'linux-armv7l'..."
      conda convert -p linux-armv7l linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-aarch64"* ]]; then
      echo "# CONVERTING PACKAGE FOR PLATFORM 'linux-aarch64'..."
      conda convert -p linux-aarch64 linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"win-32"* ]]; then
      echo "# CONVERTING PACKAGE FOR PLATFORM 'win-32'..."
      conda convert -p win-32 linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"win-64"* ]]; then
      echo "# CONVERTING PACKAGE FOR PLATFORM 'win-64'..."
      conda convert -p win-64 linux-64/*.tar.bz2
    fi

    cd ../..
}

upload_package(){

    local build_folder="${BUILD_ROOT}/build_data"

    export ANACONDA_API_TOKEN=$ANACONDA_PUSH_TOKEN
    ANACONDA_FORCE_UPLOAD=""

    if [[ "${INPUT_OVERRIDE}" == "true" ]]; then
    ANACONDA_FORCE_UPLOAD=" --force "
    fi

    if [[ "${INPUT_DRY_RUN}" == "true" ]]; then
    echo "# 'dry-run' ACTIVATED. EXITING WITHOUT PUBLISHING TO CONDA."
    exit 0
    fi

    echo "# UPLOOADING PACKAGE: linux-64..."
    anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-64/*.tar.bz2

    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"osx-64"* ]]; then
      echo "# UPLOOADING PACKAGE: osx-64..."
      anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/osx-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"osx-arm64"* ]]; then
      echo "# UPLOOADING PACKAGE: osx-arm64..."
      anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/osx-arm64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-32"* ]]; then
      echo "# UPLOOADING PACKAGE: linux-32..."
      anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-32/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-ppc64"* ]]; then
      echo "# UPLOOADING PACKAGE: linux-ppc64..."
      anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-ppc64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-ppc64le"* ]]; then
      echo "# UPLOOADING PACKAGE: linux-ppc641e..."
      anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-ppc64le/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-s390x"* ]]; then
      echo "# UPLOOADING PACKAGE: linux-s390x..."
      anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-s390x/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-armv6l"* ]]; then
      echo "# UPLOOADING PACKAGE: linux-armv6l..."
      anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-armv6l/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-armv7l"* ]]; then
      echo "# UPLOOADING PACKAGE: linux-armv7l..."
      anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-armv7l/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-aarch64"* ]]; then
      echo "# UPLOOADING PACKAGE: linux-aarch64..."
      anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-aarch64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"win-32"* ]]; then
      echo "# UPLOOADING PACKAGE: win-32..."
      anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/win-32/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"win-64"* ]]; then
      echo "# UPLOOADING PACKAGE: win-64..."
      anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/win-64/*.tar.bz2
    fi

}

echo ""
echo "# STARTING CONDA PACKAGE BUILD..."
echo "  BASE_DIR: ${INPUT_SUBDIR}"
echo "  CONDA PACKAGE VERSION: ${CONDA_PACKAGE_VERSION}"
echo "  PYTHON VERSION OF PACKAGE: ${PYTHON_VERSION}"
echo ""
go_to_build_dir
check_if_meta_yaml_template_file_exists
create_meta_yaml
build_package
upload_package
