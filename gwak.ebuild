# Copyright 1999-2021 Gentoo Authors
# Distributed under the terms of the GNU General Public License v2

EAPI=8

PYTHON_COMPAT=( python3_10 pypy3 )

DESCRIPTION="Directory gwaking utility"
HOMEPAGE="https://github.com/LiviaMedeiros/gwak"
SRC_URI="mirror://pypi/${P:0:1}/${PN}/${P}.tar.gz"

LICENSE="GPL-3"
SLOT="0"
KEYWORDS="~alpha ~amd64 ~arm ~arm64 ~hppa ~ia64 ~m68k ~mips ~ppc ~ppc64 ~riscv ~s390 ~sparc ~x86"
IUSE="yaml"

DEPEND=""
BDEPEND=""
RDEPEND="
	yaml? ( dev-python/pyyaml[${PYTHON_USEDEP}] )
"
