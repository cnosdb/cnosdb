#!/usr/bin/python3 -u
# -*- coding: UTF-8 -*-

import sys
import os
import subprocess
from datetime import datetime
import shutil
import tempfile
import hashlib
import re
import logging
import argparse

################
#### CnosDB Variables
################

# Packaging variables
PACKAGE_NAME = "cnosdb"
# INSTALL_ROOT_DIR = "/usr/bin"
# LOG_DIR = "/var/log/cnosdb"
# DATA_DIR = "/var/lib/cnosdb"
# SCRIPT_DIR = "/usr/lib/cnosdb/scripts"
# CONFIG_DIR = "/etc/cnosdb"
# LOGROTATE_DIR = "/etc/logrotate.d"
# MAN_DIR = "/usr/share/man"
#
# INIT_SCRIPT = "scripts/cnosdb/init.sh"
# SYSTEMD_SCRIPT = "scripts/cnosdb/cnosdb.service"
# PREINST_SCRIPT = "scripts/cnosdb/pre-install.sh"
# POSTINST_SCRIPT = "scripts/cnosdb/post-install.sh"
# POSTUNINST_SCRIPT = "scripts/cnosdb/post-uninstall.sh"
# LOGROTATE_SCRIPT = "scripts/cnosdb/logrotate"
# DEFAULT_CONFIG = "etc/cnosdb.sample.toml"
#
# CONFIGURATION_FILES = [
#     CONFIG_DIR + '/cnosdb.conf',
#     LOGROTATE_DIR + '/cnosdb',
# ]

PACKAGE_LICENSE = "MIT"
PACKAGE_URL = "https://github.com/cnosdb/cnosdb"
MAINTAINER = "support@github.com"
VENDOR = "CnosDB"
DESCRIPTION = "Distributed time-series database."

prereqs = ['git', 'go']
go_vet_command = "go vet ./..."
optional_prereqs = ['fpm', 'rpmbuild', 'gpg']

supported_builds = {
    'darwin': ["amd64"],
    'windows': ["amd64"],
    'linux': ["amd64", "i386", "armhf", "arm64", "armel", "static_i386", "static_amd64"]
}

supported_packages = {
    "darwin": ["tar"],
    "linux": ["deb", "rpm", "tar"],
    "windows": ["zip"],
}


################
#### CnosDB Functions
################

def print_banner():
    logging.info("""
                                  _   _
                                 | | | |
  ___   _ __     ___    ___    __| | | |__
 / __| | '_    / _   / __|  / _  | | '_
| (__  | | | | | (_) | __  | (_| | | |_) |
  ___| |_| |_|  ___/  |___/  __,_| |_.__/
  Build Script
""")


def get_package_variables(pkgKind):
    package_variables = {}
    packages = {'cnosdb'}
    if pkgKind:
        packages.add('cnosdb-meta')
    for pkg in packages:
        pkginfo = {}
        variables = {}
        variables['PACKAGE_NAME'] = "{}".format(pkg)
        variables['INSTALL_ROOT_DIR'] = "/usr/bin"
        variables['LOG_DIR'] = "/var/log/{}".format(pkg)
        variables['DATA_DIR'] = "/var/lib/{}".format(pkg)
        variables['SCRIPT_DIR'] = "/usr/lib/{}/scripts".format(pkg)
        variables['CONFIG_DIR'] = "/etc/{}".format(pkg)
        variables['LOGROTATE_DIR'] = "/etc/logrotate.d"
        variables['MAN_DIR'] = "/usr/share/man"
        variables['INIT_SCRIPT'] = "scripts/{}/init.sh".format(pkg)
        variables['SYSTEMD_SCRIPT'] = "scripts/{0}/{1}.service".format(pkg, pkg)
        variables['PREINST_SCRIPT'] = "scripts/{}/pre-install.sh".format(pkg)
        variables['POSTINST_SCRIPT'] = "scripts/{}/post-install.sh".format(pkg)
        variables['POSTUNINST_SCRIPT'] = "scripts/{}/post-uninstall.sh".format(pkg)
        variables['LOGROTATE_SCRIPT'] = "scripts/{}/logrotate".format(pkg)
        variables['DEFAULT_CONFIG'] = "etc/{}.sample.toml".format(pkg)
        variables['CONFIGURATION_FILES'] = [
            variables['CONFIG_DIR'] + '/{}.conf'.format(pkg),
            variables['LOGROTATE_DIR'] + '/{}'.format(pkg)]
        pkginfo['variables'] = variables
        targets = {}
        if pkg == 'cnosdb':
            targets['cnosdb'] ='./cmd/cnosdb'
            targets['cnosdb-cli'] ='./cmd/cnosdb-cli'
            targets['cnosdb-inspect'] = './cmd/cnosdb-inspect'
        else:
            targets['cnosdb-meta'] = './cmd/cnosdb-meta'
            targets['cnosdb-ctl'] = './cmd/cnosdb-ctl'
        pkginfo['variables'] = variables
        pkginfo['targets'] = targets
        package_variables[pkg] = pkginfo
    return package_variables


def get_fpm_common_args(info):
    fpm_common_args = "-f -s dir --log error \
    --vendor {} \
    --url {} \
    --after-install {} \
    --before-install {} \
    --after-remove {} \
    --license {} \
    --maintainer {} \
    --directories {} \
    --directories {} \
    --directories {} \
    --description \"{}\"".format(
        VENDOR,
        PACKAGE_URL,
        info['POSTINST_SCRIPT'],
        info['PREINST_SCRIPT'],
        info['POSTUNINST_SCRIPT'],
        PACKAGE_LICENSE,
        MAINTAINER,
        info['LOG_DIR'],
        info['DATA_DIR'],
        info['MAN_DIR'],
        DESCRIPTION)
    for f in info['CONFIGURATION_FILES']:
        fpm_common_args += " --config-files {}".format(f)
    return fpm_common_args


def create_package_fs(pkginfo, build_root):
    """Create a filesystem structure to mimic the package filesystem.
    """
    logging.debug("Creating package filesystem at location: {}".format(build_root))
    # Using [1:] for the path names due to them being absolute
    # (will overwrite previous paths, per 'os.path.join' documentation)
    dirs = [pkginfo['INSTALL_ROOT_DIR'][1:],
            pkginfo['LOG_DIR'][1:],
            pkginfo['DATA_DIR'][1:],
            pkginfo['SCRIPT_DIR'][1:],
            pkginfo['CONFIG_DIR'][1:],
            pkginfo['LOGROTATE_DIR'][1:],
            pkginfo['MAN_DIR'][1:]]
    for d in dirs:
        os.makedirs(os.path.join(build_root, d))
        os.chmod(os.path.join(build_root, d), 0o755)


def package_scripts(pkgname, pkginfo, build_root, config_only=False, windows=False):
    """Copy the necessary scripts and configuration files to the package
    filesystem.
    """
    if config_only:
        logging.debug("Copying configuration to build directory.")
        shutil.copyfile(pkginfo['DEFAULT_CONFIG'], os.path.join(build_root, "{}.conf".format(pkgname)))
        os.chmod(os.path.join(build_root, "{}.conf".format(pkgname)), 0o644)
    else:
        logging.debug("Copying scripts and sample configuration to build directory.")
        shutil.copyfile(pkginfo['INIT_SCRIPT'], os.path.join(build_root, pkginfo['SCRIPT_DIR'][1:], pkginfo['INIT_SCRIPT'].split('/')[2]))  # init.sh
        os.chmod(os.path.join(build_root, pkginfo['SCRIPT_DIR'][1:], pkginfo['INIT_SCRIPT'].split('/')[2]), 0o644)
        shutil.copyfile(pkginfo['SYSTEMD_SCRIPT'], os.path.join(build_root, pkginfo['SCRIPT_DIR'][1:], pkginfo['SYSTEMD_SCRIPT'].split('/')[2]))  # cnosdb.service
        os.chmod(os.path.join(build_root, pkginfo['SCRIPT_DIR'][1:], pkginfo['SYSTEMD_SCRIPT'].split('/')[2]), 0o644)
        shutil.copyfile(pkginfo['LOGROTATE_SCRIPT'], os.path.join(build_root, pkginfo['LOGROTATE_DIR'][1:], "{}".format(pkgname))) # etc/logrotate.d/cnosdb
        os.chmod(os.path.join(build_root, pkginfo['LOGROTATE_DIR'][1:], "".format(pkgname)), 0o644)
        shutil.copyfile(pkginfo['DEFAULT_CONFIG'], os.path.join(build_root, pkginfo['CONFIG_DIR'][1:], "{}.conf".format(pkgname))) # cnosdb.conf
        os.chmod(os.path.join(build_root, pkginfo['CONFIG_DIR'][1:], "{}.conf".format(pkgname)), 0o644)


def package_man_files(pkgname, pkginfo, build_root):
    """Copy and gzip man pages to the package filesystem."""
    logging.debug("Installing man pages.")
    run("make -C man/{0} clean install DESTDIR={1}/usr".format(pkgname, build_root))
    # usr/share/man
    for path, dir, files in os.walk(os.path.join(build_root, pkginfo['MAN_DIR'][1:])):
        for f in files:
            run("gzip -9n {}".format(os.path.join(path, f)))


def go_get(branch, update=False, no_uncommitted=False):
    """Retrieve build dependencies or restore pinned dependencies.
    """
    if local_changes() and no_uncommitted:
        logging.error("There are uncommitted changes in the current directory.")
        return False
    return True


def run_tests(race, parallel, timeout, no_vet, junit=False):
    """Run the Go test suite on binary output.
    """
    logging.info("Starting tests...")
    if race:
        logging.info("Race is enabled.")
    if parallel is not None:
        logging.info("Using parallel: {}".format(parallel))
    if timeout is not None:
        logging.info("Using timeout: {}".format(timeout))

    logging.info("Fetching module dependencies...")
    run("go mod download")

    logging.info("Ensuring code is properly formatted with go fmt...")
    out = run("go fmt ./...")
    if len(out) > 0:
        logging.error("Code not formatted. Please use 'go fmt ./...' to fix formatting errors.")
        logging.error("{}".format(out))
        return False
    if not no_vet:
        logging.info("Running 'go vet'...")
        out = run(go_vet_command)
        if len(out) > 0:
            logging.error("Go vet failed. Please run 'go vet ./...' and fix any errors.")
            logging.error("{}".format(out))
            return False
    else:
        logging.info("Skipping 'go vet' call...")
    test_command = "go test -v"
    if race:
        test_command += " -race"
    if parallel is not None:
        test_command += " -parallel {}".format(parallel)
    if timeout is not None:
        test_command += " -timeout {}".format(timeout)
    if junit:
        logging.info("Retrieving go-junit-report...")
        run("go get github.com/jstemmer/go-junit-report")

        # Retrieve the output from this command.
        logging.info("Running tests...")
        logging.debug("{}".format(test_command))
        proc = subprocess.Popen(test_command.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output, unused_err = proc.communicate()
        output = output.decode('utf-8').strip()

        # Process the output through go-junit-report.
        with open('test-results.xml', 'w') as f:
            logging.debug("{}".format("go-junit-report"))
            junit_proc = subprocess.Popen(["go-junit-report"], stdin=subprocess.PIPE, stdout=f, stderr=subprocess.PIPE)
            unused_output, err = junit_proc.communicate(output.encode('ascii', 'ignore'))
            if junit_proc.returncode != 0:
                logging.error("Command '{}' failed with error: {}".format("go-junit-report", err))
                sys.exit(1)

        if proc.returncode != 0:
            logging.error("Command '{}' failed with error: {}".format(test_command, output.encode('ascii', 'ignore')))
            sys.exit(1)
    else:
        logging.info("Running tests...")
        output = run(test_command)
        logging.debug("Test output:\n{}".format(out.encode('ascii', 'ignore')))
    return True


################
#### All CnosDB-specific content above this line
################

def run(command, allow_failure=False, shell=False):
    """Run shell command (convenience wrapper around subprocess).
    """
    out = None
    logging.debug("{}".format(command))
    try:
        if shell:
            out = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=shell)
        else:
            out = subprocess.check_output(command.split(), stderr=subprocess.STDOUT)
        out = out.decode('utf-8').strip()
        # logging.debug("Command output: {}".format(out))
    except subprocess.CalledProcessError as e:
        if allow_failure:
            logging.warn("Command '{}' failed with error: {}".format(command, e.output))
            return None
        else:
            logging.error("Command '{}' failed with error: {}".format(command, e.output))
            sys.exit(1)
    except OSError as e:
        if allow_failure:
            logging.warn("Command '{}' failed with error: {}".format(command, e))
            return out
        else:
            logging.error("Command '{}' failed with error: {}".format(command, e))
            sys.exit(1)
    else:
        return out


def create_temp_dir(packageName="", prefix=None):
    """ Create temporary directory with optional prefix.
    """
    if prefix is None:
        return tempfile.mkdtemp(prefix="{}-build.".format(packageName))
    else:
        return tempfile.mkdtemp(prefix=prefix)


def increment_minor_version(version):
    """Return the version with the minor version incremented and patch
    version set to zero.
    """
    ver_list = version.split('.')
    if len(ver_list) != 3:
        logging.warn(
            "Could not determine how to increment version '{}', will just use provided version.".format(version))
        return version
    ver_list[1] = str(int(ver_list[1]) + 1)
    ver_list[2] = str(0)
    inc_version = '.'.join(ver_list)
    logging.debug("Incremented version from '{}' to '{}'.".format(version, inc_version))
    return inc_version


def get_current_version_tag():
    """Retrieve the raw git version tag.
    """
    version = run("git describe --always --tags --abbrev=0")
    return version


def get_current_version():
    """Parse version information from git tag output.
    """
    version_tag = get_current_version_tag()
    # Remove leading 'v'
    if version_tag[0] == 'v':
        version_tag = version_tag[1:]
    # Replace any '-'/'_' with '~'
    if '-' in version_tag:
        version_tag = version_tag.replace("-", "~")
    if '_' in version_tag:
        version_tag = version_tag.replace("_", "~")
    return version_tag


def get_current_commit(short=False):
    """Retrieve the current git commit.
    """
    command = None
    if short:
        command = "git log --pretty=format:'%h' -n 1"
    else:
        command = "git rev-parse HEAD"
    out = run(command)
    return out.strip('\'\n\r ')


def get_current_branch():
    """Retrieve the current git branch.
    """
    command = "git rev-parse --abbrev-ref HEAD"
    out = run(command)
    return out.strip()


def local_changes():
    """Return True if there are local un-committed changes.
    """
    output = run("git diff-files --ignore-submodules --").strip()
    if len(output) > 0:
        return True
    return False


def get_system_arch():
    """Retrieve current system architecture.
    """
    arch = os.uname()[4]
    arch = "amd64"
    if arch == "x86_64":
        arch = "amd64"
    elif arch == "386":
        arch = "i386"
    elif arch == "aarch64":
        arch = "arm64"
    elif 'arm' in arch:
        # Prevent uname from reporting full ARM arch (eg 'armv7l')
        arch = "arm"
    return arch


def get_system_platform():
    """Retrieve current system platform.
    """
    if sys.platform.startswith("linux"):
        return "linux"
    else:
        return sys.platform


def get_go_version():
    """Retrieve version information for Go.
    """
    out = run("go version")
    matches = re.search('go version go(\S+)', out)
    if matches is not None:
        return matches.groups()[0].strip()
    return None


def check_path_for(b):
    """Check the the user's path for the provided binary.
    """

    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    for path in os.environ["PATH"].split(os.pathsep):
        path = path.strip('"')
        full_path = os.path.join(path, b)
        if os.path.isfile(full_path) and os.access(full_path, os.X_OK):
            return full_path


def check_environ(build_dir=None):
    """Check environment for common Go variables.
    """
    logging.info("Checking environment...")
    for v in ["GOPATH", "GOBIN", "GOROOT"]:
        logging.debug("Using '{}' for {}".format(os.environ.get(v), v))

    cwd = os.getcwd()
    if build_dir is None and os.environ.get("GOPATH") and os.environ.get("GOPATH") in cwd:
        logging.warn("Your current directory is under your GOPATH. This may lead to build failures when using modules.")
    return True


def check_prereqs():
    """Check user path for required dependencies.
    """
    logging.info("Checking for dependencies...")
    for req in prereqs:
        if not check_path_for(req):
            logging.error("Could not find dependency: {}".format(req))
            return False
    return True


def go_list(vendor=False, relative=False):
    """
    Return a list of packages
    If vendor is False vendor package are not included
    If relative is True the package prefix defined by PACKAGE_URL is stripped
    """
    p = subprocess.Popen(["go", "list", "./..."], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    packages = out.split('\n')
    if packages[-1] == '':
        packages = packages[:-1]
    if not vendor:
        non_vendor = []
        for p in packages:
            if '/vendor/' not in p:
                non_vendor.append(p)
        packages = non_vendor
    if relative:
        relative_pkgs = []
        for p in packages:
            r = p.replace(PACKAGE_URL, '.')
            if r != '.':
                relative_pkgs.append(r)
        packages = relative_pkgs
    return packages


def build(version=None,
          platform=None,
          arch=None,
          nightly=False,
          race=False,
          clean=False,
          outdir=".",
          tags=[],
          static=False,
          pkginfos={}):
    """Build each target for the specified architecture and platform.
    """
    logging.info("Starting build for {}/{}...".format(platform, arch))
    logging.info("Using Go version: {}".format(get_go_version()))
    logging.info("Using git branch: {}".format(get_current_branch()))
    logging.info("Using git commit: {}".format(get_current_commit()))
    if static:
        logging.info("Using statically-compiled output.")
    if race:
        logging.info("Race is enabled.")
    if len(tags) > 0:
        logging.info("Using build tags: {}".format(','.join(tags)))

    logging.info("Sending build output to: {}".format(outdir))
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    elif clean and outdir != '/' and outdir != ".":
        logging.info("Cleaning build directory '{}' before building.".format(outdir))
        shutil.rmtree(outdir)
        os.makedirs(outdir)

    logging.info("Using version '{}' for build.".format(version))

    for pkg, pkginfo in pkginfos.items():
        for target, path in pkginfo['targets'].items():
            logging.info("Building target: {}".format(target))
            build_command = ""

            # Handle static binary output
            if static is True or "static_" in arch:
                if "static_" in arch:
                    static = True
                    arch = arch.replace("static_", "")
                build_command += "CGO_ENABLED=0 "

            # Handle variations in architecture output
            if arch == "i386" or arch == "i686":
                arch = "386"
            elif "arm" in arch:
                arch = "arm"
            build_command += "GOOS={} GOARCH={} ".format(platform, arch)

            if "arm" in arch:
                if arch == "armel":
                    build_command += "GOARM=5 "
                elif arch == "armhf" or arch == "arm":
                    build_command += "GOARM=6 "
                elif arch == "arm64":
                    # TODO - Verify this is the correct setting for arm64
                    build_command += "GOARM=7 "
                else:
                    logging.error("Invalid ARM architecture specified: {}".format(arch))
                    logging.error("Please specify either 'armel', 'armhf', or 'arm64'.")
                    return False
            if platform == 'windows':
                target = target + '.exe'
            build_command += "go build -o {} ".format(os.path.join(outdir, target))
            if race:
                build_command += "-race "
            if len(tags) > 0:
                build_command += "-tags {} ".format(','.join(tags))
            if "1.4" in get_go_version():
                if static:
                    build_command += "-ldflags=\"-s -X main.version {} -X main.branch {} -X main.commit {}\" ".format(
                        version,
                        get_current_branch(),
                        get_current_commit())
                else:
                    build_command += "-ldflags=\"-X main.version {} -X main.branch {} -X main.commit {}\" ".format(version,
                                                                                                                   get_current_branch(),
                                                                                                                   get_current_commit())

            else:
                # Starting with Go 1.5, the linker flag arguments changed to 'name=value' from 'name value'
                if static:
                    build_command += "-ldflags=\"-s -X main.version={} -X main.branch={} -X main.commit={}\" ".format(
                        version,
                        get_current_branch(),
                        get_current_commit())
                else:
                    build_command += "-ldflags=\"-X main.version={} -X main.branch={} -X main.commit={}\" ".format(version,
                                                                                                                   get_current_branch(),
                                                                                                                   get_current_commit())
            if static:
                build_command += "-a -installsuffix cgo "
            build_command += path
            start_time = datetime.utcnow()
            run(build_command, shell=True)
            end_time = datetime.utcnow()
            logging.info("Time taken: {}s".format((end_time - start_time).total_seconds()))
    return True


def generate_md5_from_file(path):
    """Generate MD5 signature based on the contents of the file at path.
    """
    m = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            m.update(chunk)
    return m.hexdigest()


def generate_sig_from_file(path):
    """Generate a detached GPG signature from the file at path.
    """
    logging.debug("Generating GPG signature for file: {}".format(path))
    gpg_path = check_path_for('gpg')
    if gpg_path is None:
        logging.warn("gpg binary not found on path! Skipping signature creation.")
        return False
    if os.environ.get("GNUPG_HOME") is not None:
        run('gpg --homedir {} --armor --yes --detach-sign {}'.format(os.environ.get("GNUPG_HOME"), path))
    else:
        run('gpg --armor --detach-sign --yes {}'.format(path))
    return True


def package(build_output, pkg_name, version, nightly=False, iteration=1, static=False, release=False, pkginfos={}):
    """Package the output of the build process.
    """
    try:
        outfiles = []
        for pkgname, info in pkginfos.items():
            pkg_name = pkgname
            logging.info("This pkg_name is {}".format(pkg_name))
            logging.info("Start build package...")
            tmp_build_dir = create_temp_dir(pkg_name)
            logging.debug("Packaging for build output: {}".format(build_output))  # {'linux': {'amd64': '/opt/cnosdb/build'}}
            logging.info("Using temporary directory: {}".format(tmp_build_dir))  # /tmp/cnosdb-build.hzlw1ekk
            for platform in build_output:  # platform: linux
                # Create top-level folder displaying which platform (linux, etc)
                os.makedirs(os.path.join(tmp_build_dir, platform))
                for arch in build_output[platform]:
                    logging.info("Creating packages for {}/{}".format(platform, arch))  # linux, amd
                    # Create second-level directory displaying the architecture (amd64, etc)
                    current_location = build_output[platform][arch]

                    # Create directory tree to mimic file system of package
                    build_root = os.path.join(tmp_build_dir,
                                              platform,
                                              arch,
                                              '{}-{}-{}'.format(pkg_name, version, iteration))
                    os.makedirs(build_root)

                    # Copy packaging scripts to build directory
                    if platform == "windows":  # 如果是Windows系统
                        # For windows and static builds, just copy
                        # binaries to root of package (no other scripts or
                        # directories)
                        package_scripts(pkgname, info['variables'], build_root, config_only=True, windows=True)

                    elif static or "static_" in arch:
                        package_scripts(pkgname, info['variables'], build_root, config_only=True)
                    else:
                        create_package_fs(info['variables'], build_root)
                        package_scripts(pkgname, info['variables'], build_root)

                    if platform != "windows":
                        package_man_files(pkgname, info['variables'], build_root)

                    for binary in info['targets']:
                        # Copy newly-built binaries to packaging directory
                        if platform == 'windows':
                            binary = binary + '.exe'
                        if platform == 'windows' or static or "static_" in arch:
                            # Where the binary should go in the package filesystem
                            to = os.path.join(build_root, binary)
                            # Where the binary currently is located
                            fr = os.path.join(current_location, binary)
                        else:
                            # Where the binary currently is located
                            fr = os.path.join(current_location, binary)
                            # Where the binary should go in the package filesystem
                            to = os.path.join(build_root, info['variables']['INSTALL_ROOT_DIR'][1:], binary)
                        shutil.copy(fr, to)

                    for package_type in supported_packages[platform]:
                        # Package the directory structure for each package type for the platform
                        logging.debug("Packaging directory '{}' as '{}'.".format(build_root, package_type))
                        name = pkg_name
                        # Reset version, iteration, and current location on each run
                        # since they may be modified below.
                        package_version = version
                        package_iteration = iteration
                        if "static_" in arch:
                            # Remove the "static_" from the displayed arch on the package
                            package_arch = arch.replace("static_", "")
                        else:
                            package_arch = arch
                        if not release and not nightly:
                            # For non-release builds, just use the commit hash as the version
                            package_version = "{}~{}".format(version,
                                                             get_current_commit(short=True))
                            package_iteration = "0"
                        package_build_root = build_root
                        current_location = build_output[platform][arch]

                        if package_type in ['zip', 'tar']:
                            # For tars and zips, start the packaging one folder above
                            # the build root (to include the package name)
                            package_build_root = os.path.join('/', '/'.join(build_root.split('/')[:-1]))
                            if nightly:
                                if static or "static_" in arch:
                                    name = '{}-static-nightly_{}_{}'.format(name,
                                                                            platform,
                                                                            package_arch)
                                else:
                                    name = '{}-nightly_{}_{}'.format(name,
                                                                     platform,
                                                                     package_arch)
                            else:
                                if static or "static_" in arch:
                                    name = '{}-{}-static_{}_{}'.format(name,
                                                                       package_version,
                                                                       platform,
                                                                       package_arch)
                                else:
                                    name = '{}-{}_{}_{}'.format(name,
                                                                package_version,
                                                                platform,
                                                                package_arch)
                            current_location = os.path.join(os.getcwd(), current_location)
                            if package_type == 'tar':
                                tar_command = "cd {} && tar -cvzf {}.tar.gz --owner=root ./*".format(package_build_root,
                                                                                                     name)
                                run(tar_command, shell=True)
                                run("mv {}.tar.gz {}".format(os.path.join(package_build_root, name), current_location),
                                    shell=True)
                                outfile = os.path.join(current_location, name + ".tar.gz")
                                outfiles.append(outfile)
                            elif package_type == 'zip':
                                zip_command = "cd {} && zip -r {}.zip ./*".format(package_build_root, name)
                                run(zip_command, shell=True)
                                run("mv {}.zip {}".format(os.path.join(package_build_root, name), current_location),
                                    shell=True)
                                outfile = os.path.join(current_location, name + ".zip")
                                outfiles.append(outfile)
                        elif package_type not in ['zip', 'tar'] and static or "static_" in arch:
                            logging.info("Skipping package type '{}' for static builds.".format(package_type))
                        else:
                            fpm_common_args = get_fpm_common_args(info['variables'])
                            fpm_command = "fpm {} --name {} -a {} -t {} --version {} --iteration {} -C {} -p {} ".format(
                                fpm_common_args,
                                name,
                                package_arch,
                                package_type,
                                package_version,
                                package_iteration,
                                package_build_root,
                                current_location)
                            if package_type == "rpm":
                                fpm_command += "--depends coreutils --depends shadow-utils --rpm-posttrans {}".format(
                                    info['variables']['POSTINST_SCRIPT'])
                            out = run(fpm_command, shell=True)
                            matches = re.search(':path=>"(.*)"', out)
                            outfile = None
                            if matches is not None:
                                outfile = matches.groups()[0]
                            if outfile is None:
                                logging.warn("Could not determine output from packaging output!")
                            else:
                                if nightly:
                                    # Strip nightly version from package name
                                    new_outfile = outfile.replace("{}-{}".format(package_version, package_iteration),
                                                                  "nightly")
                                    os.rename(outfile, new_outfile)
                                    outfile = new_outfile
                                else:
                                    if package_type == 'rpm':
                                        # rpm's convert any dashes to underscores
                                        package_version = package_version.replace("-", "_")
                                    new_outfile = outfile.replace("{}-{}".format(package_version, package_iteration),
                                                                  package_version)
                                    os.rename(outfile, new_outfile)
                                    outfile = new_outfile
                                outfiles.append(os.path.join(os.getcwd(), outfile))
            logging.debug("Produced package files: {}".format(outfiles))
        return outfiles
    finally:
        # Cleanup
        shutil.rmtree(tmp_build_dir)


def main(args):
    packageInfos = get_package_variables(args.single)

    if args.release and args.nightly:
        logging.error("Cannot be both a nightly and a release.")
        return 1

    if args.nightly:
        args.version = increment_minor_version(args.version)
        args.version = "{}~n{}".format(args.version,
                                       datetime.utcnow().strftime("%Y%m%d%H%M"))
        args.iteration = 0

    # Pre-build checks
    check_environ()
    if not check_prereqs():
        return 1
    if args.build_tags is None:
        args.build_tags = []
    else:
        args.build_tags = args.build_tags.split(',')

    orig_commit = get_current_commit(short=True)
    orig_branch = get_current_branch()

    if args.platform not in supported_builds and args.platform != 'all':
        logging.error("Invalid build platform: {}".format(args.platform))
        return 1

    build_output = {}

    if args.branch != orig_branch and args.commit != orig_commit:
        logging.error("Can only specify one branch or commit to build from.")
        return 1
    elif args.branch != orig_branch:
        logging.info("Moving to git branch: {}".format(args.branch))
        run("git checkout {}".format(args.branch))
    elif args.commit != orig_commit:
        logging.info("Moving to git commit: {}".format(args.commit))
        run("git checkout {}".format(args.commit))

    if not args.no_get:
        if not go_get(args.branch, update=args.update, no_uncommitted=args.no_uncommitted):
            return 1

    if args.test:
        if not run_tests(args.race, args.parallel, args.timeout, args.no_vet, args.junit_report):
            return 1

    # platforms = []
    single_build = True
    if args.platform == 'all':
        platforms = supported_builds.keys()
        single_build = False
    else:
        platforms = [args.platform]

    for platform in platforms:
        build_output.update({platform: {}})
        archs = []
        if args.arch == "all":
            single_build = False
            archs = supported_builds.get(platform)
        else:
            archs = [args.arch]
        for arch in archs:
            od = args.outdir
            if not single_build:
                od = os.path.join(args.outdir, platform, arch)
            if not build(version=args.version,
                         platform=platform,
                         arch=arch,
                         nightly=args.nightly,
                         race=args.race,
                         clean=args.clean,
                         outdir=od,
                         tags=args.build_tags,
                         static=args.static,
                         pkginfos=packageInfos):
                return 1
            build_output.get(platform).update({arch: od})

    # Build packages
    if args.package:
        if not check_path_for("fpm"):
            logging.error("FPM ruby gem required for packaging. Stopping.")
            return 1
        packages = package(build_output,
                           args.name,
                           args.version,
                           nightly=args.nightly,
                           iteration=args.iteration,
                           static=args.static,
                           release=args.release,
                           pkginfos=packageInfos)
        if args.sign:
            logging.debug("Generating GPG signatures for packages: {}".format(packages))
            sigs = []  # retain signatures so they can be uploaded with packages
            for p in packages:
                if generate_sig_from_file(p):
                    sigs.append(p + '.asc')
                else:
                    logging.error("Creation of signature for package [{}] failed!".format(p))
                    return 1
            packages += sigs
        logging.info("Packages created:")
        for p in packages:
            logging.info("{} (MD5={})".format(p.split('/')[-1:][0],
                                              generate_md5_from_file(p)))
    if orig_branch != get_current_branch():
        logging.info("Moving back to original git branch: {}".format(orig_branch))
        run("git checkout {}".format(orig_branch))

    return 0


if __name__ == '__main__':
    LOG_LEVEL = logging.INFO
    if '--debug' in sys.argv[1:]:
        LOG_LEVEL = logging.DEBUG
    log_format = '[%(levelname)s] %(funcName)s: %(message)s'
    logging.basicConfig(level=LOG_LEVEL,
                        format=log_format)

    parser = argparse.ArgumentParser(description='CnosDB build and packaging script.')
    parser.add_argument('--verbose', '-v', '--debug',
                        action='store_true',
                        help='Use debug output')
    parser.add_argument('--outdir', '-o',
                        metavar='<output directory>',
                        default='./build',
                        type=os.path.abspath,
                        help='Output directory')
    parser.add_argument('--name', '-n',
                        metavar='<name>',
                        default=PACKAGE_NAME,
                        type=str,
                        help='Name to use for package name (when package is specified)')
    parser.add_argument('--arch',
                        metavar='<amd64|i386|armhf|arm64|armel|all>',
                        type=str,
                        default=get_system_arch(),
                        help='Target architecture for build output')
    parser.add_argument('--platform',
                        metavar='<linux|darwin|windows|all>',
                        type=str,
                        default=get_system_platform(),
                        help='Target platform for build output')
    parser.add_argument('--branch',
                        metavar='<branch>',
                        type=str,
                        default=get_current_branch(),
                        help='Build from a specific branch')
    parser.add_argument('--commit',
                        metavar='<commit>',
                        type=str,
                        default=get_current_commit(short=True),
                        help='Build from a specific commit')
    parser.add_argument('--version',
                        metavar='<version>',
                        type=str,
                        default=get_current_version(),
                        help='Version information to apply to build output (ex: 0.12.0)')
    parser.add_argument('--iteration',
                        metavar='<package iteration>',
                        type=str,
                        default="1",
                        help='Package iteration to apply to build output (defaults to 1)')
    parser.add_argument('--stats',
                        action='store_true',
                        help='Emit build metrics (requires CnosDB Python client)')
    parser.add_argument('--stats-server',
                        metavar='<hostname:port>',
                        type=str,
                        help='Send build stats to CnosDB using provided hostname and port')
    parser.add_argument('--stats-db',
                        metavar='<database name>',
                        type=str,
                        help='Send build stats to CnosDB using provided database name')
    parser.add_argument('--nightly',
                        action='store_true',
                        help='Mark build output as nightly build (will incremement the minor version)')
    parser.add_argument('--update',
                        action='store_true',
                        help='Update build dependencies prior to building')
    parser.add_argument('--package',
                        action='store_false',
                        help='Package binary output')
    parser.add_argument('--release',
                        action='store_true',
                        help='Mark build output as release')
    parser.add_argument('--clean',
                        action='store_true',
                        help='Clean output directory before building')
    parser.add_argument('--no-get',
                        action='store_true',
                        help='Do not retrieve pinned dependencies when building')
    parser.add_argument('--no-uncommitted',
                        action='store_true',
                        help='Fail if uncommitted changes exist in the working directory')
    parser.add_argument('--build-tags',
                        metavar='<tags>',
                        help='Optional build tags to use for compilation')
    parser.add_argument('--static',
                        action='store_true',
                        help='Create statically-compiled binary output')
    parser.add_argument('--sign',
                        action='store_true',
                        help='Create GPG detached signatures for packages (when package is specified)')
    parser.add_argument('--test',
                        action='store_true',
                        help='Run tests (does not produce build output)')
    parser.add_argument('--junit-report',
                        action='store_true',
                        help='Output tests in the JUnit XML format')
    parser.add_argument('--no-vet',
                        action='store_true',
                        help='Do not run "go vet" when running tests')
    parser.add_argument('--race',
                        action='store_true',
                        help='Enable race flag for build output')
    parser.add_argument('--parallel',
                        metavar='<num threads>',
                        type=int,
                        help='Number of tests to run simultaneously')
    parser.add_argument('--timeout',
                        metavar='<timeout>',
                        type=str,
                        help='Timeout for tests before failing')
    parser.add_argument('--single',
                        action='store_false',
                        help='Execute the meta packaging process')
    args = parser.parse_args()
    print_banner()
    sys.exit(main(args))
