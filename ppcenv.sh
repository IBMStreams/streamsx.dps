#!/bin/sh

function my_augment_path {
  if [[ $(expr match $PATH ".*$1") == 0 ]]; then 
    export PATH="$1":$PATH;
  fi
}

# Set PATH variables
arch=`uname -m`

# Only add AT for PPC builds
if [[ $arch = "ppc64le" ]] || [[ $arch = "ppc64" ]] ; then
    if [[ -d "/opt/at8.0" ]] && [[ $arch = "ppc64le" ]] ; then
        my_augment_path "/opt/at8.0/bin"
        my_augment_path "/opt/at8.0/bin64"
    elif [[ -d "/opt/at7.1" ]] ; then
        my_augment_path "/opt/at7.1/bin"
        my_augment_path "/opt/at7.1/bin64"
    elif [[ -d "/opt/at7.0" ]] ; then
        my_augment_path "/opt/at7.0/bin"
        my_augment_path "/opt/at7.0/bin64"
    elif [[ -d "/opt/at5.0" ]] ; then
        my_augment_path "/opt/at5.0/bin"
        my_augment_path "/opt/at5.0/bin64"
    else
        echo "No AT compiler installed."
    fi        
fi
