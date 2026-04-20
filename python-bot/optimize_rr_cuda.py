"""GPU-backed candidate scorer for optimize_rr.

Per cell, we build one GPU-resident layout: each unique window appears
ONCE (deduplicated across folds), with per-fold (is_train, is_val)
membership matrices. Candidates are scored in batches: a (K, E) mask
tensor on the flat entry array, first-match-per-window via
scatter_reduce(amin), per-trade P&L, then aggregates split by the
fold/set membership matrices.

Returns tuples in the same shape as _cv_score_candidate so the
downstream ranking code in optimize_rr.main() needs no changes.
"""

from __future__ import annotations

import math
import os
from typing import Any, Optional


def is_available() -> bool:
    try:
        import torch
        return torch.cuda.is_available()
    except Exception:
        return False


def _build_slippage_table(get_slippage_cents_fn, device):
    import torch
    table = torch.zeros(101, dtype=torch.float32, device=device)
    for p in range(101):
        table[p] = get_slippage_cents_fn(p) / 100.0
    return table


def build_cell_tensors(pp_by_bucket, train_sets, device: str = "cuda"):
    """Flatten all UNIQUE windows in the cell into a single GPU layout.
    Each window appears once; per-fold membership is stored in two
    (n_folds, W) bool matrices so a window that lives in 7 training
    folds + 1 val fold doesn't get replicated 8×.

    Windows are identified by their object id — pp_by_bucket[f] and
    train_sets[f] share the same dict objects that came out of
    preprocess_window().
    """
    import torch

    device_t = torch.device(device)
    n_folds = len(pp_by_bucket)

    # Dedupe windows by id(), preserving first-seen order.
    unique: list[dict] = []
    win_index: dict[int, int] = {}
    is_train_rows: list[list[int]] = [[0] * 0 for _ in range(n_folds)]
    is_val_rows: list[list[int]] = [[0] * 0 for _ in range(n_folds)]

    def _intern(pp: dict) -> int:
        key = id(pp)
        idx = win_index.get(key)
        if idx is None:
            idx = len(unique)
            win_index[key] = idx
            unique.append(pp)
        return idx

    # Collect memberships first (flat per-fold lists of window indices).
    train_idx_per_fold: list[list[int]] = []
    val_idx_per_fold: list[list[int]] = []
    for f in range(n_folds):
        tr = [_intern(pp) for pp in train_sets[f]]
        va = [_intern(pp) for pp in pp_by_bucket[f]]
        train_idx_per_fold.append(tr)
        val_idx_per_fold.append(va)

    W = len(unique)

    # Build dense (n_folds, W) membership matrices.
    is_train = torch.zeros((n_folds, W), dtype=torch.bool, device=device_t)
    is_val = torch.zeros((n_folds, W), dtype=torch.bool, device=device_t)
    for f in range(n_folds):
        if train_idx_per_fold[f]:
            is_train[f, torch.tensor(train_idx_per_fold[f], device=device_t)] = True
        if val_idx_per_fold[f]:
            is_val[f, torch.tensor(val_idx_per_fold[f], device=device_t)] = True

    # Discover momentum keys.
    mom_keys: set[tuple[int, int]] = set()
    for pp in unique:
        for e in pp["entries"]:
            m = e.get("momentum")
            if m:
                mom_keys.update(m.keys())
    mom_keys_list = sorted(mom_keys)

    # Stream features into python lists then upload once.
    all_secs: list[float] = []
    all_entry_px: list[float] = []
    all_fav: list[float] = []
    all_buffer: list[float] = []
    all_vol: list[float] = []
    all_side_yes: list[int] = []
    all_window_idx: list[int] = []
    all_entry_pos: list[int] = []
    mom_arrays: dict[tuple[int, int], list[float]] = {k: [] for k in mom_keys_list}
    window_start: list[int] = []
    window_result: list[int] = []

    NAN = float("nan")
    offset = 0
    for w_id, pp in enumerate(unique):
        window_start.append(offset)
        window_result.append(1 if pp["result"] == "yes" else 0)
        for pos, e in enumerate(pp["entries"]):
            all_secs.append(float(e["secs_left"]))
            all_entry_px.append(float(e["entry_price"]))
            all_fav.append(float(e["fav_price"]))
            buf = e["buffer_pct"]
            all_buffer.append(NAN if buf is None else float(buf))
            vol = e["realized_vol"]
            all_vol.append(NAN if vol is None else float(vol))
            all_side_yes.append(1 if e["side"] == "yes" else 0)
            all_window_idx.append(w_id)
            all_entry_pos.append(pos)
            m = e.get("momentum") or {}
            for k in mom_keys_list:
                v = m.get(k)
                mom_arrays[k].append(NAN if v is None else float(v))
            offset += 1
    E = offset

    def _t(data, dtype):
        return torch.tensor(data, dtype=dtype, device=device_t)

    return {
        "E": E,
        "W": W,
        "n_folds": n_folds,
        "mom_keys": mom_keys_list,
        "secs": _t(all_secs, torch.float32),
        "entry_px": _t(all_entry_px, torch.float32),
        "fav": _t(all_fav, torch.float32),
        "buffer": _t(all_buffer, torch.float32),
        "vol": _t(all_vol, torch.float32),
        "side_yes": _t(all_side_yes, torch.bool),
        "window_idx": _t(all_window_idx, torch.int64),
        "entry_pos": _t(all_entry_pos, torch.int32),
        "momentum": {k: _t(mom_arrays[k], torch.float32) for k in mom_keys_list},
        "window_start": _t(window_start, torch.int64),
        "window_result": _t(window_result, torch.bool),
        "is_train": is_train,  # (n_folds, W)
        "is_val": is_val,      # (n_folds, W)
        "device": device_t,
    }


def score_candidates(
    cell_tensors: dict,
    candidates: list[dict],
    get_slippage_cents_fn,
    wilson_lower_bound_fn,
    min_val_trades_per_fold: int,
    min_total_val_trades: int,
    recency_halflife_folds: float,
    batch_size: int = 2048,
    progress_fn=None,
) -> list[Optional[tuple]]:
    """Score every candidate, returning a list of tuples whose shape
    matches _cv_score_candidate. None for candidates that fail the
    per-fold / total-trade gates."""
    import torch

    device = cell_tensors["device"]
    E = cell_tensors["E"]
    W = cell_tensors["W"]
    n_folds = cell_tensors["n_folds"]

    slip_table = _build_slippage_table(get_slippage_cents_fn, device)

    secs_e = cell_tensors["secs"].unsqueeze(0)
    entry_px_e = cell_tensors["entry_px"].unsqueeze(0)
    fav_e = cell_tensors["fav"].unsqueeze(0)
    buffer_e = cell_tensors["buffer"].unsqueeze(0)
    vol_e = cell_tensors["vol"].unsqueeze(0)
    side_yes_e = cell_tensors["side_yes"].unsqueeze(0)
    entry_pos_e = cell_tensors["entry_pos"].unsqueeze(0)
    window_idx = cell_tensors["window_idx"]
    window_start = cell_tensors["window_start"]
    window_result = cell_tensors["window_result"]
    is_train = cell_tensors["is_train"]   # (n_folds, W) bool
    is_val = cell_tensors["is_val"]       # (n_folds, W) bool

    time_scale = torch.sqrt(torch.clamp(secs_e, min=1.0) / 60.0)
    vol_finite = ~torch.isnan(vol_e)
    buf_finite = ~torch.isnan(buffer_e)

    INF_PLUS = torch.tensor(E + 1, dtype=cell_tensors["entry_pos"].dtype, device=device)

    results: list[Optional[tuple]] = [None] * len(candidates)

    rw = torch.tensor(
        [0.5 ** ((n_folds - 1 - i) / recency_halflife_folds) for i in range(n_folds)],
        dtype=torch.float64,
    ).numpy()

    for batch_start in range(0, len(candidates), batch_size):
        batch = candidates[batch_start:batch_start + batch_size]
        K = len(batch)

        min_cp = torch.tensor([p["min_contract_price"] for p in batch], dtype=torch.float32, device=device).unsqueeze(1)
        max_ep = torch.tensor([p["max_entry_price"] for p in batch], dtype=torch.float32, device=device).unsqueeze(1)
        min_secs_p = torch.tensor([p.get("min_seconds", 10) for p in batch], dtype=torch.float32, device=device).unsqueeze(1)
        max_secs_p = torch.tensor([p["max_seconds"] for p in batch], dtype=torch.float32, device=device).unsqueeze(1)
        min_buf = torch.tensor([p["min_price_buffer_pct"] for p in batch], dtype=torch.float32, device=device).unsqueeze(1)
        max_mom = torch.tensor([p["max_adverse_momentum"] for p in batch], dtype=torch.float32, device=device).unsqueeze(1)
        max_vol = torch.tensor(
            [p["max_realized_vol_pct"] if p.get("max_realized_vol_pct") is not None else float("inf")
             for p in batch],
            dtype=torch.float32, device=device,
        ).unsqueeze(1)

        mask = (secs_e >= min_secs_p) & (secs_e <= max_secs_p)
        mask &= (fav_e >= min_cp)
        mask &= (entry_px_e >= min_cp) & (entry_px_e <= max_ep)
        mask &= ~(vol_finite & (vol_e > max_vol))

        required = min_buf * time_scale
        reject_buf = buf_finite & (
            (side_yes_e & (buffer_e < required)) |
            (~side_yes_e & (buffer_e > -required))
        )
        mask &= ~reject_buf

        key_to_idxs: dict[tuple[int, int], list[int]] = {}
        for i, p in enumerate(batch):
            key = (p.get("momentum_window", 60), p.get("momentum_periods", 5))
            key_to_idxs.setdefault(key, []).append(i)
        for key, idxs in key_to_idxs.items():
            mom_arr = cell_tensors["momentum"].get(key)
            if mom_arr is None:
                continue
            idxs_t = torch.tensor(idxs, device=device, dtype=torch.long)
            sub_max_mom = max_mom[idxs_t]
            apply = (sub_max_mom < 0)
            mom_e = mom_arr.unsqueeze(0)
            mom_finite = ~torch.isnan(mom_e)
            reject_mom = (
                apply & mom_finite & (
                    (side_yes_e & (mom_e < sub_max_mom)) |
                    (~side_yes_e & (mom_e > -sub_max_mom))
                )
            )
            mask[idxs_t] = mask[idxs_t] & ~reject_mom

        # first_pos[k, w] = min entry-position matching per window
        scores = torch.where(mask, entry_pos_e, INF_PLUS)
        first_pos = torch.full((K, W), E + 1, dtype=cell_tensors["entry_pos"].dtype, device=device)
        idx_expand = window_idx.unsqueeze(0).expand(K, -1)
        first_pos.scatter_reduce_(
            dim=1, index=idx_expand, src=scores, reduce="amin", include_self=True,
        )
        has_match = first_pos < (E + 1)

        # Drop the large (K, E) intermediates before building (K, W) P&L.
        del scores, mask, idx_expand, required, reject_buf

        global_idx = window_start.unsqueeze(0) + first_pos.to(torch.int64)
        global_idx = torch.where(has_match, global_idx, torch.zeros_like(global_idx))
        flat = global_idx.view(-1)
        match_price = cell_tensors["entry_px"].index_select(0, flat).view(K, W)
        match_side_yes = cell_tensors["side_yes"].index_select(0, flat).view(K, W)

        contracts = torch.clamp(torch.floor(1000.0 / match_price.clamp(min=1.0)), min=1.0)
        stake_kw = contracts * match_price / 100.0
        slip = contracts * slip_table[match_price.clamp(min=0.0, max=100.0).long()]
        win_profit_kw = contracts * (100.0 - match_price) / 100.0 - slip
        loss_profit_kw = -contracts * match_price / 100.0 - slip
        won = has_match & (match_side_yes == window_result.unsqueeze(0))
        profit_kw = torch.where(won, win_profit_kw, loss_profit_kw)
        profit_kw = torch.where(has_match, profit_kw, torch.zeros_like(profit_kw))
        stake_kw = torch.where(has_match, stake_kw, torch.zeros_like(stake_kw))

        # Per-fold aggregates via (K, W) × (n_folds, W) membership:
        # results (K, n_folds) = sum over W of (match & membership[f, w]).
        has_match_f = has_match.to(torch.float32)
        won_f = won.to(torch.float32)
        val_trades_mat = has_match_f @ is_val.to(torch.float32).T         # (K, n_folds)
        val_wins_mat = won_f @ is_val.to(torch.float32).T
        val_profit_mat = profit_kw.to(torch.float64) @ is_val.to(torch.float64).T
        val_stake_mat = stake_kw.to(torch.float64) @ is_val.to(torch.float64).T
        val_win_profit_mat = torch.where(won, profit_kw, torch.zeros_like(profit_kw)).to(torch.float64) \
            @ is_val.to(torch.float64).T
        val_loss_profit_mat = torch.where(has_match & ~won, profit_kw, torch.zeros_like(profit_kw)).to(torch.float64) \
            @ is_val.to(torch.float64).T
        train_trades_mat = has_match_f @ is_train.to(torch.float32).T
        train_wins_mat = won_f @ is_train.to(torch.float32).T
        train_profit_mat = profit_kw.to(torch.float64) @ is_train.to(torch.float64).T

        pft = val_trades_mat.round().long().cpu().numpy()
        pfw = val_wins_mat.round().long().cpu().numpy()
        pfp = val_profit_mat.cpu().numpy()
        pfs = val_stake_mat.cpu().numpy()
        pfwp = val_win_profit_mat.cpu().numpy()
        pflp = val_loss_profit_mat.cpu().numpy()
        ptt = train_trades_mat.round().long().cpu().numpy()
        ptw = train_wins_mat.round().long().cpu().numpy()
        ptp = train_profit_mat.cpu().numpy()

        for k in range(K):
            trades_per_fold = pft[k]
            if min_val_trades_per_fold > 0 and (trades_per_fold < min_val_trades_per_fold).any():
                continue
            total_val_trades = int(trades_per_fold.sum())
            if total_val_trades < min_total_val_trades:
                continue

            wins_per_fold = pfw[k]
            profit_per_fold = pfp[k]
            stake_per_fold = pfs[k]
            win_profit_per_fold = pfwp[k]
            loss_profit_per_fold = pflp[k]
            train_trades_per_fold = ptt[k]
            train_wins_per_fold = ptw[k]
            train_profit_per_fold = ptp[k]

            total_val_wins = int(wins_per_fold.sum())
            total_val_losses = total_val_trades - total_val_wins
            val_wr = total_val_wins / total_val_trades

            total_train_trades = int(train_trades_per_fold.sum())
            train_wr = (float(train_wins_per_fold.sum()) / total_train_trades) if total_train_trades > 0 else 0.0

            total_val_profit = float(profit_per_fold.sum())
            total_val_stake = float(stake_per_fold.sum())
            total_val_win_profit = float(win_profit_per_fold.sum())
            total_val_loss_profit = float(loss_profit_per_fold.sum())

            weighted_wins = float((wins_per_fold * rw).sum())
            weighted_total = float((trades_per_fold * rw).sum())
            wr_lb = wilson_lower_bound_fn(
                int(round(weighted_wins)),
                max(1, int(round(weighted_total))),
            )

            avg_win = total_val_win_profit / total_val_wins if total_val_wins > 0 else 0.0
            if total_val_losses > 0:
                avg_loss = total_val_loss_profit / total_val_losses
            else:
                avg_stake = total_val_stake / total_val_trades if total_val_trades > 0 else 0.0
                avg_loss = -avg_stake
            safe_ppt = wr_lb * avg_win + (1 - wr_lb) * avg_loss

            # Flat numpy array instead of a list of nested dicts.
            # Columns: [val_trades, val_profit, train_profit] per fold.
            # Downstream code reads only those three values. The lean
            # dict version was ~8KB per viable candidate × millions of
            # candidates = OOM. This is ~250 bytes.
            import numpy as np
            fold_results = np.empty((n_folds, 3), dtype=np.float64)
            fold_results[:, 0] = trades_per_fold
            fold_results[:, 1] = profit_per_fold
            fold_results[:, 2] = train_profit_per_fold

            results[batch_start + k] = (
                wr_lb, val_wr, total_val_losses, total_val_trades,
                train_wr, total_train_trades, batch[k], fold_results,
                safe_ppt, total_val_profit,
            )

        if progress_fn is not None:
            progress_fn(batch_start + K, len(candidates))

        del first_pos, has_match, global_idx, flat
        del match_price, match_side_yes, contracts, stake_kw, slip
        del win_profit_kw, loss_profit_kw, won, profit_kw
        del val_trades_mat, val_wins_mat, val_profit_mat, val_stake_mat
        del val_win_profit_mat, val_loss_profit_mat
        del train_trades_mat, train_wins_mat, train_profit_mat

    return results
