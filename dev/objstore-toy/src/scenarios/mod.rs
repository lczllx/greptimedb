// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! 场景集合。每个场景 = 一个子命令 = 一条可判定的假设。
//!
//! **PASS/FAIL 的统一语义：`PASS` 表示「计划中写下的假设被实测证实」。**
//! 所以 PASS **不一定**是"系统安全"—— 比如 S1 的 PASS 意思是
//! 「ABA 洞确实存在」，那是个**坏消息**被证实。
//!
//! 场景清单与判定信号见 `README.md`。

pub mod s0;
pub mod s1;
pub mod s10;
pub mod s11;
pub mod s12;
pub mod s13;
pub mod s15;
pub mod s16;
pub mod s17;
pub mod s2;
pub mod s3;
pub mod s4;
pub mod s5;
pub mod s6;
pub mod s7;
pub mod s8;
pub mod s9;
