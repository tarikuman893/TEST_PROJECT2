<?php

namespace App\Console\Commands\ImportMeasure;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportMeasureMp extends Command
{
    protected $signature   = 'import:measure-mp';
    protected $description = 'MP 計測 CSV を measure_mp へ取込む';

    /* ───── ユーティリティ ───── */
    private static function toDate(?string $v): ?string
    {
        $v = trim($v ?? '');
        if ($v === '' || $v === '0000-00-00 00:00:00') return null;
        $ts = @strtotime($v);
        return $ts === false ? null : date('Y-m-d H:i:s', $ts);
    }
    private static function toInt(?string $v): int
    {
        return (int) str_replace(',', '', $v ?? '0');
    }
    /** 税込→税抜（10 % 想定） */
    private static function net(int $gross): int
    {
        return (int) round($gross / 1.1, 0, PHP_ROUND_HALF_UP);
    }

    public function handle(): int
    {
        $src = storage_path('app/csvA8/Measure/mp_measure.csv');

        foreach (glob($src) as $csv) {
            /* SJIS→UTF-8 変換（BOM 対応） */
            $tmp = tempnam(sys_get_temp_dir(), 'mp_') . '.csv';
            file_put_contents($tmp, mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'auto'));

            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()

                /* ───── 除外 ───── */
                ->reject(function (array $r): bool {
                    array_pop($r);                                           // 末尾ダミー 1 列削除
                    if (empty($r) || strpos($r[0] ?? '', 'タイプ') !== false) return true;

                    if (in_array($r[4] ?? '', ['2025/05/29 17:59:14','2025/05/28 15:11:56'], true)) return true;
                    if (in_array($r[3] ?? '', ['2025/07/18 17:00:52','2025/07/18 10:24:42','2025/07/17 17:31:26'], true)) return true;
                    foreach ([
                        '2025-07-16 10:23:34','2025-07-15 18:46:10','2025-07-15 08:43:48',
                        '2025-07-15 08:03:19','2025-07-14 17:51:03','2025-07-13 10:24:20',
                        '2025-07-12 23:53:32','2025-07-11 15:03:20','2025-07-09 22:22:37',
                        '2025-07-09 08:14:30','2025-07-07 11:44:08','2025-07-05 20:54:31',
                        '2025-07-05 16:32:07'
                    ] as $ng) if (($r[4] ?? '') === $ng) return true;

                    return false;
                })

                /* ───── 取込本体 ───── */
                ->each(function (array $r): void {

                    /* 計測 ID／TCLICK 抽出 */
                    $p = $r[8] ?? '';
                    $parts = str_contains($p, '_YCLID_') ? explode('_YCLID_', $p, 2)
                           : (str_contains($p, '_CLID_') ? explode('_CLID_',  $p, 2)
                           :                               explode('_GCLID_',  $p, 2));

                    $mid = ''; $tclick = '';
                    if (!empty($parts[0]) && str_contains($parts[0], '_TCLICK_'))
                        [$mid, $tclick] = explode('_TCLICK_', $parts[0], 2);
                    else $mid = $parts[0] ?? '';

                    /* gclid / utm_content */
                    $gclid = ''; $utm = '';
                    if (!empty($parts[1]) && str_contains($parts[1], '_UTMC_'))
                        [$gclid, $utm] = explode('_UTMC_', $parts[1], 2);
                    else $gclid = $parts[1] ?? '';

                    /* UUID → unknown */
                    if ($mid !== '' && preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i', $mid))
                        $mid = "unknown（{$r[1]}）";

                    /* MOTA 補完 */
                    if (($r[1] ?? '') === '車査定/株式会社MOTA/MOTA車査定' && ($r[12] ?? '') === 'MOTA_Meta' && $mid === '')
                        $mid = 'fb_usedcar-1_mota_mp';

                    /* ミシュワン ID 是正 */
                    if (($r[3] ?? '') === '2025/07/01 14:23:42')
                        $mid = 'gs_mishone_mp_TCLICK__CjwKZJs…';

                    /* サイト名補正（元ロジック） */
                    if (($r[12] ?? '') === 'YS_葉酸サプリ-2')                    $r[12] = '葉酸サプリ安心ランキング';
                    if (in_array($r[12] ?? '', ['ミシュワン_消化LP','ミシュワン_アレルギーLP'], true)) $r[12] = 'ミシュワン_通常';
                    if (str_contains($p,'gs_dogfood-1_mishone_mp') && ($r[12] ?? '') === 'ミシュワン_通常')
                        $r[12] = '転職エージェント評判｜-BEST WORK-';
                    if (str_contains($mid,'ys_dogfood-1')) $r[12] = 'ドッグフード（yahoo）';
                    if (str_contains($mid,'ys_dogfood-2')) $r[12] = 'ドッグフード（yahoo2）';
                    if (str_contains($p,'gs_dogfood-2'))    $r[12] .= '（google2）';
                    if (str_contains($p,'ms_dogfood-1'))
                        $r[12] = ($r[12]==='ミシュワン_通常'?'ミシュワン_マイクロソフト':$r[12]).'（microsoft）';

                    /* 固定報酬補正（カードローン系） */
                    $fix = 0;
                    if (($r[2] ?? '') < '2025-05-20') {
                        if      (str_contains($mid,'acom'))    $fix = 12000;
                        elseif  (str_contains($mid,'promise')) $fix = 14000;
                        elseif  (str_contains($mid,'aiful'))   $fix =  9000;
                        elseif  (str_contains($mid,'mobit'))   $fix = 14000;
                    } else {
                        if      (str_contains($mid,'acom'))    $fix = 75000;
                        elseif  (str_contains($mid,'promise')) $fix = 14000;
                        elseif  (str_contains($mid,'aiful'))   $fix =  9000;
                        elseif  (str_contains($mid,'mobit'))   $fix = 14000;
                    }
                    if ($fix) $r[2] = (string)$fix;           // 先に固定値へ置換

                    /* ===== ここで税抜計算 ===== */
                    $rewardNet = self::net(self::toInt($r[2] ?? null));

                    /* 空 ID 行は無視 */
                    if ($mid === '') return;

                    /* pg_name = プログラム名 + サイト名（連結・空白なし） */
                    $pgNameConcat = ($r[1] ?? '') . ($r[12] ?? '');

                    /* INSERT：device=9 / referer=10 / keyword=11 / site_name=12 */
                    DB::table('measure_mp')->insertOrIgnore([
                        'archive_type'        => $r[0]  ?? null,
                        'pg_name'             => $pgNameConcat !== '' ? $pgNameConcat : null,
                        'reward'              => $rewardNet,
                        'click_time'          => self::toDate($r[3]  ?? null),
                        'occur_time'          => self::toDate($r[4]  ?? null),
                        'fix_time'            => self::toDate($r[5]  ?? null),
                        'fix_time_new_column' => self::toDate($r[6]  ?? null),
                        'statement'           => $r[7]  ?? null,
                        'user_id'             => $mid,
                        'gclid'               => $gclid ?: null,
                        'utm_content'         => $utm   ?: null,
                        'device'              => $r[9]  ?? null,
                        'referer'             => $r[10] ?? null,
                        'keyword'             => $r[11] ?? null,   // ファイルには値なし
                        'site_name'           => $r[12] ?? null,
                        'tclick'              => $tclick ?: null,
                    ]);
                });

            unlink($tmp);
        }

        $this->info('MP 計測データ取込完了');
        return Command::SUCCESS;
    }
}
