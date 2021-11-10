#!/usr/bin/env bash

# adopted from: https://github.com/MichaelsJP/conda-package-publish-action
# License: MIT

set -ex
set -o pipefail

INPUT_SUBDIR=$1
INPUT_VERSION=$2
INPUT_DRY_RUN=$3
if [ -z "$INPUT_DRY_RUN" ]; then
  INPUT_DRY_RUN="true"
fi

echo $INPUT_DRY_RUN

ANACONDA_USER="dharpa"
INPUT_OVERRIDE="false"
INPUT_PLATFORMS="all"

CHANNEL_ARGS="-c conda-forge -c dharpa"

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
    if [ -z $INPUT_VERSION ]; then
        echo "No version specified."
        exit 1
    fi
    sed "s/__VERSION__/${INPUT_VERSION}/" meta.yaml.template >> meta.yaml
}

build_package(){

    local PYTHON_VERSION=$1
    local build_folder="build_${PYTHON_VERSION}"

    rm -rf "${build_folder}"

    # Build for Linux
    conda build --py ${PYTHON_VERSION} ${CHANNEL_ARGS} --output-folder "${build_folder}" .

    cd "${build_folder}"

    # Convert to other platforms
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"osx-64"* ]]; then
    conda convert -p osx-64 linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"osx-arm64"* ]]; then
    conda convert -p osx-arm64 linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-32"* ]]; then
    conda convert -p linux-32 linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-ppc64"* ]]; then
    conda convert -p linux-ppc64 linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-ppc64le"* ]]; then
    conda convert -p linux-ppc64le linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-s390x"* ]]; then
    conda convert -p linux-s390x linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-armv6l"* ]]; then
    conda convert -p linux-armv6l linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-armv7l"* ]]; then
    conda convert -p linux-armv7l linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-aarch64"* ]]; then
    conda convert -p linux-aarch64 linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"win-32"* ]]; then
    conda convert -p win-32 linux-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"win-64"* ]]; then
    conda convert -p win-64 linux-64/*.tar.bz2
    fi

    cd ..
}

upload_package(){

    local PYTHON_VERSION=$1
    local build_folder="build_${PYTHON_VERSION}"

    export ANACONDA_API_TOKEN=$ANACONDA_PUSH_TOKEN
    ANACONDA_FORCE_UPLOAD=""

    if [[ "${INPUT_OVERRIDE}" == "true" ]]; then
    ANACONDA_FORCE_UPLOAD=" --force "
    fi

    if [[ "${INPUT_DRY_RUN}" == "true" ]]; then
    echo "Dry Run activated. Exiting without publishing to conda."
    exit 0
    fi

    anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-64/*.tar.bz2

    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"osx-64"* ]]; then
    anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/osx-64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"osx-arm64"* ]]; then
    anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/osx-arm64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-32"* ]]; then
    anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-32/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-ppc64"* ]]; then
    anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-ppc64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-ppc64le"* ]]; then
    anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-ppc64le/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-s390x"* ]]; then
    anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-s390x/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-armv6l"* ]]; then
    anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-armv6l/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-armv7l"* ]]; then
    anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-armv7l/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"linux-aarch64"* ]]; then
    anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/linux-aarch64/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"win-32"* ]]; then
    anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/win-32/*.tar.bz2
    fi
    if [[ $INPUT_PLATFORMS == *"all"* || $INPUT_PLATFORMS == *"win-64"* ]]; then
    anaconda upload -u ${ANACONDA_USER} $ANACONDA_FORCE_UPLOAD --label main ${build_folder}/win-64/*.tar.bz2
    fi

}

go_to_build_dir
check_if_meta_yaml_template_file_exists
create_meta_yaml
build_package 3.9 &
build_package 3.10 &
wait
upload_package 3.9
upload_package 3.10
