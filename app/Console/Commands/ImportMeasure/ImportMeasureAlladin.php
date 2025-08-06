<?php

namespace App\Console\Commands\ImportMeasure;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportMeasureAlladin extends Command
{
    protected $signature   = 'import:measure-alladin';
    protected $description = 'Alladin 計測 CSV を measure_alladin へ取込む';

    /* ---------- 共通ヘルパ ---------- */
    private static function toDate(?string $v): ?string
    {
        $v = trim($v ?? '');
        if ($v === '' || $v === '0000-00-00 00:00:00') return null;
        $ts = @strtotime(str_replace('/', '-', $v));
        return $ts === false ? null : date('Y-m-d H:i:s', $ts);
    }

    private static function toInt(?string $v): int
    {
        return (int) str_replace(',', '', $v ?? '0');
    }

    /* ---------- Alladin 固有ステップ ---------- */
    private static function alladin_measure_01_isBlank(array $l): bool
    {
        return !(($l[0] ?? '') === '' || ($l[0] ?? '') === "\x1A");
    }

    private static function alladin_measure_02_excludeId(array $l): bool
    {
        return !in_array($l[0] ?? '', [
            'nmobre1002158532',
            'nmobre1001344706',
            'nmobre1000050064',
            'nmobre1000060286',
        ], true);
    }

    private static function alladin_measure_03_correctId(array &$l): void
    {
        if (in_array($l[0], [
            'nmobre1001125844',
            'nmobre1001116368',
            'nmobre1001115062',
        ], true)) {
            $l[1] = 'ys_dogfood-2_obremo1_ald';
        } elseif ($l[0] === 'nmobre0000033684') {
            $l[1] = 'sbggobremo';
        }
    }

    private static function alladin_measure_04_fixReward(array &$l): void
    {
        if ((int)($l[7] ?? 0) === 0 && ($l[5] ?? '') === '31956') {
            $l[7] = 16000;
        }
    }

    /* ---------- 分割ユーティリティ ---------- */
    private static function splitIds(string $measureField, string $utmcField): array
    {
        $measureId = $measureField;
        $tclickId  = '';
        if (strpos($measureId, '_TCLICK_') !== false) {
            [$measureId, $tclickId] = explode('_TCLICK_', $measureId, 2);
        }

        $gclid = $utmcField;
        $utm   = '';
        if (strpos($gclid, '_UTMC_') !== false) {
            [$gclid, $utm] = explode('_UTMC_', $gclid, 2);
        }

        return [$measureId, $tclickId, $gclid, $utm];
    }

    /* ---------- 行フォーマット ---------- */
    private static function formatRow(array $line)
    {
        // Alladin 用ステップを自動収集
        $steps = array_filter(
            get_class_methods(self::class),
            fn($m) => str_starts_with($m, 'alladin_measure_')
        );
        natsort($steps); // 01,02… の順に実行

        foreach ($steps as $step) {
            $res = self::$step($line);    // $line は参照渡し
            if ($res === false) return false; // スキップ判定
        }

        [$measureId, $tclickId, $gclid, $utm] =
            self::splitIds($line[1] ?? '', $line[13] ?? '');

        return [
            'order_id'      => $line[0]      ?? null,
            'measure_id'    => $measureId     ?: null,
            'click_time'    => self::toDate($line[2] ?? null),
            'occur_time'    => self::toDate($line[3] ?? null),
            'fix_time'      => self::toDate($line[4] ?? null),
            'media_id'      => $line[5]      ?? null,
            'media_name'    => $line[6]      ?? null,
            'reward'        => $line[7]      ?? null,
            'transaction'   => $line[8]      ?? null,
            'reward_rate'   => $line[9]      ?? null,
            'statement'     => $line[10]     ?? null,
            'device'        => $line[11]     ?? null,
            'referer'       => $line[12]     ?? null,
            'gclid'         => $gclid         ?: null,
            'user_id'       => $utm           ?: null,
            'tclick'        => $tclickId      ?: null,
        ];
    }

    /* ---------- メイン ---------- */
    public function handle(): int
    {
        $sourceDir  = storage_path('app/csv/Measure');
        $csvPattern = "$sourceDir/*alladin*.csv";
        $headers    = ['成果識別子'];

        foreach (glob($csvPattern) as $csv) {
            $tmp = tempnam(sys_get_temp_dir(), 'csv_') . '.csv';
            file_put_contents(
                $tmp,
                mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'SJIS-win,CP932,UTF-8')
            );

            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()
                ->reject(fn($row) => empty($row) || in_array($row[0] ?? '', $headers, true))
                ->each(function (array $row) {
                    $row = self::formatRow($row);
                    if ($row === false) return;

                    DB::table('measure_alladin')->insert($row);
                });

            unlink($tmp);
        }

        $this->info('Alladin 計測データ取込完了');
        return Command::SUCCESS;
    }
}
