#!/usr/bin/perl
# begin_generated_IBM_copyright_prolog                             
#                                                                  
# This is an automatically generated copyright prolog.             
# After initializing,  DO NOT MODIFY OR MOVE                       
# **************************************************************** 
# Licensed Materials - Property of IBM                             
# 5724-Y95                                                         
# (C) Copyright IBM Corp.  2009, 2015    All Rights Reserved.      
# US Government Users Restricted Rights - Use, duplication or      
# disclosure restricted by GSA ADP Schedule Contract with          
# IBM Corp.                                                        
#                                                                  
# end_generated_IBM_copyright_prolog                               

use strict;
use Getopt::Long qw(:config no_ignore_case bundling) ;
use File::Basename;
use Cwd qw(abs_path getcwd);

sub usage
{
	print "Usage: " . basename($0) . " [--help] [--osname] [--osname_rpm_format] [--osname_long] [--osver] [--arch] [--arch_uname] [--gccver] [--atver] [--javaver] [--antver]\n";
        print "\n";        
        print "This program is a centralized way of determing the operating system and architecture\n";
        print "of the machine it's being run on.\n";
        print "\n";
        print "If information cannot be determined, no text is printed, and a non-zero return\n";
        print "code is returned on exit.\n";
        print "\n";
        print "Options:\n";
        print "\n";        
        print "--osname             Print the operating system name (short format) of this machine.\n";
        print "                       Known supported operating system names:\n";
        print "                       rhel  - RedHat Enterprise Linux\n";
        print "                       sles  - SUSE Linux Enterprise\n";
        print "                       fc    - Fedora Core\n";
        print "--osname_rpm_format  Print the operating system name (RPM format) of this machine.\n";
        print "                       Known supported operating system names:\n";
        print "                       elx    - RedHat Enterprise Linux major version x\n";
        print "                       slesx  - SUSE Linux Enterprise major version x\n";
        print "                       fcx    - Fedora Core major version x\n";
        print "--osname_long        Print the operating system name (long format) of this machine.\n";
        print "                       (this is the contents of the /etc/xxxx-release file)\n";
        print "--osver              Print the operating system version (x.x) of this machine.\n";
        print "--arch               Print the architecture of this machine.  The architecture is\n";
        print "                       \"normalized\" to a more standard convention.\n";
        print "                        - x86\n";
        print "                        - x86_64\n";
        print "                        - ppc\n";
        print "                        - ppc64\n";
        print "--arch_uname         Print the architecture returned directly from the uname command.\n";
        print "                       The uname -i output is unmodified.\n";
        print "                        - i386\n";
        print "                        - x86_64\n";
        print "                        - ppc\n";
        print "                        - ppc64\n";
        print "--gcc-version        Print version number of the GCC compiler installed on the system.\n";
        print "--at-version         Print the version number of the Advance Toolchain Compiler Runtime.\n";
        print "                     If no version is printed, system is not using the Advanced Toolchain.\n";
        print "--javaver            Print version number of the Java SDK installed on the system.\n";
        print "--javacver           Print version number of the javac compiler installed on the system.\n";
        print "--antver             Print version number of Apache Ant installed on the system.\n";
        print "\n";
        print "Examples:\n";
        print "\n";
        print basename($0) . " --osname\n";
        print "rhel\n\n";
        print basename($0) . " --osname_rpm_format\n";
        print "el5\n\n";
        print basename($0) . " --osver\n";
        print "5.3\n\n";
        print basename($0) . " --arch\n";
        print "x86_64\n\n";
        print basename($0) . " --arch_uname\n";
        print "x86_64\n\n";
        print basename($0) . " --gccver\n";
        print "4.1.2\n\n";
        print basename($0) . " --atver\n";
        print "4.0-2\n\n";
        print basename($0) . " --javaver\n";
        print "1.6.0\n\n";
	exit;
}

# Options
my $HELP = 0;
my $OSNAME = 0;
my $OSVER = 0;
my $OSNAME_RPM_FORMAT = 0;
my $OSNAME_LONG = 0;
my $ARCH = 0;
my $ARCH_UNAME = 0;
my $GCCVER = 0;
my $ATVER = 0;
my $JAVAVER = 0;
my $JAVACVER = 0;
my $ANTVER = 0;

my $result = GetOptions("h|help"            => \$HELP,
                        "osname"            => \$OSNAME,
                        "osname_rpm_format" => \$OSNAME_RPM_FORMAT,
                        "osname_long"       => \$OSNAME_LONG,
                        "osver"             => \$OSVER,
                        "arch"              => \$ARCH,
                        "arch_uname"        => \$ARCH_UNAME,
                        "gccver"            => \$GCCVER,
                        "atver"             => \$ATVER,
                        "javaver"           => \$JAVAVER,
                        "javacver"          => \$JAVACVER,
                        "antver"            => \$ANTVER);


if (!$result || $HELP)
{
    usage();
    exit($result ? 0 : 1);
}

if ($OSNAME || $OSNAME_RPM_FORMAT || $OSNAME_LONG || $OSVER)
{
    my $osinfo;
    my $return_osname;
    my $return_osname_rpm;
    my $return_osname_long;
    my $return_osver;
    
    if (-e "/etc/redhat-release")
    {
        $return_osname = "rhel";
        
        $osinfo = `cat /etc/redhat-release`;
        if ($osinfo =~ /release (5\.\d)[^(]+\(Tikanga\)/)
        {
            $return_osname_rpm = "el5";
            $return_osver = $1;
        }
        elsif ($osinfo =~ /release (6\.\d)[^(]+\(Santiago\)/)
        {
            $return_osname_rpm = "el6";
            $return_osver = $1;            
        }
        elsif ($osinfo =~ /release (7\.\d)[^(]+\(Maipo\)/)
        {
            $return_osname_rpm = "el7";
            $return_osver = $1;
        }
        else
        {
            print STDERR "Unsupported RedHat version: $osinfo";
            exit 1;
        }        
    }
    elsif (-e "/etc/SuSE-release")
    {
        $return_osname = "sles";
        
        $osinfo = `cat /etc/SuSE-release`;
        if ($osinfo =~ m/SUSE Linux Enterprise Server/)
        {
            if ($osinfo =~ m/VERSION = (\d+)/)
            {
                $return_osname_rpm = "sles$1";
                $return_osver = $1;
            }
        }
        else
        {
            print STDERR "Unsupported SLES version: $osinfo";
            exit 1;
        }        
    }
    elsif (-e "/etc/fedora-release")
    {
        $osinfo = `cat /etc/fedora-release`;
        $return_osname = "fc";
        
        if ($osinfo =~ /Fedora release 8 (Werewolf)/)
        {
            $return_osname_rpm = "fc";
            $return_osver = "8.0";
        }
        else
        {
            print STDERR "Unsupported Fedora version: $osinfo\n";
            exit 1;
        }        
    }
    elsif (-e "/etc/debian_version")
    {
        $osinfo = `cat /etc/debian_version`;
        chomp($osinfo);
        $return_osname = "deb";
        $return_osname_rpm = "deb";
                
        if ($osinfo =~ /^(\d+\.\d+)/)
        {
            $return_osver = $1;
        }
        else
        {
            print STDERR "Unsupported Debian version: $osinfo\n";
            exit 1;
        }        
    }
    elsif (-e "/etc/issue")
    {
#         $osinfo = `cat /etc/issue`;
        
#         if ($osinfo =~ /Debian\D+(\d+\.\d+)/)
#         {
#             $return_osver = $1;
#             $return_osname = "deb";
#             $return_osname_rpm = "deb";
#         }
#         else
#         {
#             print STDERR "Unsupported OS: $osinfo\n";
#             exit 1;
#         }        
    }
    
    $return_osname_long = $osinfo;
    
    if ($OSNAME)
    {
        print "$return_osname\n";
    }
    elsif ($OSNAME_RPM_FORMAT)
    {
        print "$return_osname_rpm\n";
    }
    elsif ($OSNAME_LONG)
    {
        print "$return_osname_long\n";
    }
    elsif ($OSVER)
    {
        print "$return_osver\n";
    }
}
elsif ($ARCH)
{
    my $return_arch = `uname -i`;
    #chomp($return_arch);
    if ($return_arch =~/unknown/)
    {
        $return_arch = `uname -m`;
        #chomp($return_arch);
    }
        
    # Convert ix86 to x86
    $return_arch =~ s/i\d86/x86/;
    print "$return_arch";
}
elsif ($ARCH_UNAME)
{
    my $return_arch = `uname -i`;
    if ($return_arch =~/unknown/)
    {
        $return_arch = `uname -m`;
    }
    
    print "$return_arch";
}
elsif ($GCCVER)
{
    my $gccver = `gcc -v 2>&1`;
    $gccver =~ m/gcc version (\d+\.\d+\.\d+)/;
    print "$1\n";
}
elsif ($ATVER)
{
    my $atver = `gcc -v 2>&1`;
    $atver =~ m/Advance-Toolchain-([^)]+)/;
    print "$1\n";
}
elsif ($JAVAVER)
{
    my $javaver = `java -version 2>&1`;
    $javaver =~ m/(\d+\.\d+\.\d+)/;
    print "$1\n";
}
elsif ($JAVACVER)
{
    my $javacver = `javac -version 2>&1`;
    $javacver =~ m/(\d+\.\d+\.\d+)/;
    print "$1\n";
}
elsif ($ANTVER)
{
    my $antver = `ant -version 2>&1`;
    $antver =~ m/(\d+\.\d+\.\d+)/;
    print "$1\n";
}
else
{
    print STDERR "Unknown parameter specified.\n";
    usage();
    exit -1;
}

exit 0;


