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

//! 对象存储元数据层 tiny toy —— 去风险原型。
//!
//! 场景清单与判定信号见 `README.md`，全部结论见 `FINDINGS.md`。

pub mod backpressure;
pub mod cfg;
pub mod commit;
pub mod group_commit;
pub mod kv;
pub mod leader_kv;
pub mod scenarios;
pub mod tree;
