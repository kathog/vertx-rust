#[cfg(feature = "hz")]
extern crate conan;
#[cfg(feature = "hz")]
use conan::*;
#[cfg(feature = "hz")]
use std::path::Path;
#[cfg(feature = "hz")]
use cmake::Config;

fn main() {
    #[cfg(feature = "hz")]
        {
            let profiles = get_profile_list();
            if profiles.is_empty() {
                return;
            }
            let conan_profile = &profiles[0];
            let command = InstallCommandBuilder::new()
                .with_profile(conan_profile)
                .build_policy(BuildPolicy::Missing)
                .recipe_path(Path::new("conanfile.txt"))
                .build();

            let build_info = command.generate().unwrap();
            // let hz_include = build_info.get_dependency("hazelcast-cpp-client").unwrap().get_include_dir().unwrap();
            // let boost_include = build_info.get_dependency("boost").unwrap().get_include_dir().unwrap();

            let dst = Config::new("./")
                .cxxflag("-O3")
                .build();

            println!("cargo:rustc-link-search=native={}", dst.display());
            build_info.cargo_emit();
            println!("cargo:rustc-link-lib=dylib=stdc++");
            // println!("cargo:rustc-link-lib=hzlib");
        }

}
